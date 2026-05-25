#!/usr/bin/env bash
set -euo pipefail

N="${N:-5}"
RESHARD_MODES="${RESHARD_MODES:-legacy atomic}"
RESHARD_MODES="${RESHARD_MODES//,/ }"
NS="vk"
IMAGE="${MEMTIER_IMAGE:-memtier_k8s:1}"
LOCAL_OUT="${1:-./results/reshard}"
REMOTE_OUT="/work/results/reshard"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "${SCRIPT_DIR}/pod_results.sh"

HELM_CHART_PATH="${HELM_CHART_PATH:-../valkey-helm/valkey}"
VALUES_FILE="${VALUES_FILE:-./k8s/manifests/values.yaml}"

HOST="valkey.vk.svc.cluster.local"
PORT=6379
ADMIN_HOST="valkey-0.valkey-headless.${NS}.svc.cluster.local"
CLUSTER_ENDPOINT="${ADMIN_HOST}:${PORT}"
THREADS=4
CLIENTS=16
TEST_TIME="${TEST_TIME:-120}"
KEYS=100000
DATA_SIZE=1024
RATIO="1:1"
STEADY_STATE_WAIT=30
AUTO_REBALANCE_TIMEOUT="${AUTO_REBALANCE_TIMEOUT:-180}"
CURRENT_RESHARD_MODE=""
ORIGINAL_MASTER_IDS_SNAPSHOT=""

ORIGINAL_SHARDS=3
TARGET_SHARDS=4
REPLICAS_PER_SHARD="${REPLICAS_PER_SHARD:-1}"
ORIGINAL_NODE_COUNT=$((ORIGINAL_SHARDS * (1 + REPLICAS_PER_SHARD)))
TARGET_NODE_COUNT=$((TARGET_SHARDS * (1 + REPLICAS_PER_SHARD)))
NEW_NODE_START="${ORIGINAL_NODE_COUNT}"
NEW_NODE_END=$((TARGET_NODE_COUNT - 1))

mkdir -p "${LOCAL_OUT}"

get_master_node_id() {
  local pod="$1"
  kubectl exec "${pod}" -n "${NS}" -- valkey-cli cluster myid 2>/dev/null | tr -d '[:space:]'
}

cluster_info() {
  kubectl exec valkey-0 -n "${NS}" -- valkey-cli cluster info 2>/dev/null
}

cluster_nodes() {
  kubectl exec valkey-0 -n "${NS}" -- valkey-cli cluster nodes 2>/dev/null
}

validate_reshard_mode() {
  local mode="$1"

  case "${mode}" in
    legacy|atomic)
      ;;
    *)
      echo "ERROR: Unsupported RESHARD_MODES entry '${mode}'. Use legacy, atomic, or both." >&2
      return 1
      ;;
  esac
}

is_atomic_mode() {
  [[ "${CURRENT_RESHARD_MODE}" == "atomic" ]]
}

atomic_mode_json() {
  if is_atomic_mode; then
    echo "true"
  else
    echo "false"
  fi
}

slot_migration_command_name() {
  if is_atomic_mode; then
    echo "cluster_rebalance_atomic"
  else
    echo "cluster_rebalance"
  fi
}

cluster_info_field() {
  local field="$1"
  cluster_info | awk -F: -v field="${field}" '$1 == field {gsub(/\r/, "", $2); print $2}'
}

cluster_check() {
  kubectl exec valkey-0 -n "${NS}" -- \
    valkey-cli --cluster check "${CLUSTER_ENDPOINT}" 2>&1
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
  echo "  Running valkey-cli --cluster fix to clear open/importing/migrating slots..."
  kubectl exec valkey-0 -n "${NS}" -- \
    valkey-cli --cluster fix "${CLUSTER_ENDPOINT}" \
      --cluster-yes 2>&1
}

wait_cluster_healthy() {
  local max_wait="${1:-300}"
  local waited=0
  local state slots_ok slots_assigned check_output

  echo "  Waiting for cluster_state=ok, all slots covered, and no open slots (max ${max_wait}s)..."
  while [[ "${waited}" -lt "${max_wait}" ]]; do
    state="$(cluster_info_field cluster_state || true)"
    slots_ok="$(cluster_info_field cluster_slots_ok || true)"
    slots_assigned="$(cluster_info_field cluster_slots_assigned || true)"

    if [[ "${state}" == "ok" && "${slots_ok}" == "16384" && "${slots_assigned}" == "16384" ]]; then
      if check_output="$(cluster_check)"; then
        return 0
      fi
    fi

    sleep 5
    waited=$((waited + 5))
  done

  echo "ERROR: Cluster did not become healthy after ${max_wait}s" >&2
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

patch_rollout_partition() {
  local partition="$1"
  echo "  Setting StatefulSet rolling-update partition=${partition}"
  kubectl patch sts valkey -n "${NS}" --type=merge \
    -p "{\"spec\":{\"updateStrategy\":{\"type\":\"RollingUpdate\",\"rollingUpdate\":{\"partition\":${partition}}}}}" \
    >/dev/null
}

wait_for_pods_ready() {
  local start="$1"
  local end="$2"
  local ordinal

  for ordinal in $(seq "${start}" "${end}"); do
    kubectl wait "pod/valkey-${ordinal}" -n "${NS}" \
      --for=condition=Ready \
      --timeout=300s
  done
}

wait_for_pods_absent() {
  local start="$1"
  local end="$2"
  local timeout="${3:-300}"
  local ordinal deadline

  for ordinal in $(seq "${start}" "${end}"); do
    deadline=$((SECONDS + timeout))
    while kubectl get "pod/valkey-${ordinal}" -n "${NS}" >/dev/null 2>&1; do
      if (( SECONDS >= deadline )); then
        echo "ERROR: pod/valkey-${ordinal} still exists after ${timeout}s" >&2
        return 1
      fi
      sleep 2
    done
  done
}

extra_pods_present() {
  local ordinal

  for ordinal in $(seq "${NEW_NODE_START}" "${NEW_NODE_END}"); do
    if kubectl get "pod/valkey-${ordinal}" -n "${NS}" >/dev/null 2>&1; then
      return 0
    fi
  done

  return 1
}

wait_for_new_nodes_known() {
  local max_wait="${1:-120}"
  local waited=0
  local ordinal nodes found

  echo "  Waiting for new nodes valkey-${NEW_NODE_START}..valkey-${NEW_NODE_END} to join cluster gossip..."
  while [[ "${waited}" -lt "${max_wait}" ]]; do
    nodes="$(cluster_nodes || true)"
    found=0

    for ordinal in $(seq "${NEW_NODE_START}" "${NEW_NODE_END}"); do
      if grep -q "valkey-${ordinal}\.valkey-headless" <<<"${nodes}"; then
        found=$((found + 1))
      fi
    done

    if [[ "${found}" -eq "$((NEW_NODE_END - NEW_NODE_START + 1))" ]]; then
      return 0
    fi

    sleep 2
    waited=$((waited + 2))
  done

  echo "ERROR: New nodes did not join cluster gossip after ${max_wait}s" >&2
  cluster_nodes || true
  return 1
}

node_id_for_pod() {
  local pod="$1"
  cluster_nodes | awk -v pod="${pod}" '$0 ~ pod "\\.valkey-headless" {print $1; exit}'
}

node_flags_for_pod() {
  local pod="$1"
  cluster_nodes | awk -v pod="${pod}" '$0 ~ pod "\\.valkey-headless" {print $3; exit}'
}

node_host_for_id() {
  local node_id="$1"
  cluster_nodes | awk -v node_id="${node_id}" '
    $1 == node_id {
      split($2, bus, "@")
      split(bus[1], host_port, ":")
      print host_port[1]
      exit
    }
  '
}

original_master_ids() {
  if [[ -n "${ORIGINAL_MASTER_IDS_SNAPSHOT}" ]]; then
    printf "%s\n" ${ORIGINAL_MASTER_IDS_SNAPSHOT}
    return 0
  fi

  cluster_nodes | awk -v max_ordinal="${NEW_NODE_START}" '
    /master/ && !/fail/ {
      for (i = 0; i < max_ordinal; i++) {
        if ($0 ~ "valkey-" i "\\.valkey-headless") {
          print $1
          break
        }
      }
    }
  '
}

slot_ranges_for_node_id() {
  local node_id="$1"
  local slots_needed="$2"

  cluster_nodes | awk -v node_id="${node_id}" -v slots_needed="${slots_needed}" '
    $1 == node_id {
      remaining = slots_needed
      for (i = 9; i <= NF; i++) {
        token = $i
        if (remaining <= 0) {
          break
        }
        if (token !~ /^[0-9]+(-[0-9]+)?$/) {
          continue
        }
        if (token ~ /-/) {
          split(token, range, "-")
          start = range[1] + 0
          end = range[2] + 0
        } else {
          start = token + 0
          end = token + 0
        }
        count = end - start + 1
        if (count > remaining) {
          end = start + remaining - 1
          count = remaining
        }
        print start, end, count
        remaining -= count
      }
    }
  '
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

reset_auto_rebalance_state() {
  AUTO_REBALANCE_DETECTED=false
  AUTO_REBALANCE_STATUS="skipped"
  AUTO_REBALANCE_START=0
  AUTO_REBALANCE_END=0
  AUTO_REBALANCE_DURATION=0
  AUTO_REBALANCE_FINAL_SLOTS=0
}

observe_auto_rebalance() {
  local new_master_id="$1"
  local expected_slots="$2"
  local max_wait="$3"
  local memtier_start="$4"
  local trace_file="$5"
  local waited=0 now slots state slots_ok slots_assigned

  AUTO_REBALANCE_DETECTED=false
  AUTO_REBALANCE_STATUS="timeout"
  AUTO_REBALANCE_START=0
  AUTO_REBALANCE_END=0
  AUTO_REBALANCE_DURATION=0
  AUTO_REBALANCE_FINAL_SLOTS=0

  echo "timestamp,second,slots_on_extra_master,cluster_state,cluster_slots_ok,cluster_slots_assigned" > "${trace_file}"
  echo "  Observing chart auto-rebalance for up to ${max_wait}s..."

  while [[ "${waited}" -le "${max_wait}" ]]; do
    now="$(date +%s)"
    slots="$(slot_count_for_node_id "${new_master_id}" || echo 0)"
    state="$(cluster_info_field cluster_state || echo unknown)"
    slots_ok="$(cluster_info_field cluster_slots_ok || echo unknown)"
    slots_assigned="$(cluster_info_field cluster_slots_assigned || echo unknown)"
    AUTO_REBALANCE_FINAL_SLOTS="${slots}"

    printf "%s,%s,%s,%s,%s,%s\n" \
      "${now}" "$((now - memtier_start))" "${slots}" "${state}" "${slots_ok}" "${slots_assigned}" \
      >> "${trace_file}"

    if [[ "${slots}" -ge "${expected_slots}" ]] && cluster_is_healthy_now; then
      AUTO_REBALANCE_END="${now}"
      if [[ "${AUTO_REBALANCE_DETECTED}" == "false" ]]; then
        AUTO_REBALANCE_DETECTED=true
        AUTO_REBALANCE_STATUS="already_complete"
        AUTO_REBALANCE_START="${now}"
      else
        AUTO_REBALANCE_STATUS="complete"
      fi
      AUTO_REBALANCE_DURATION=$((AUTO_REBALANCE_END - AUTO_REBALANCE_START))
      echo "  Auto-rebalance ${AUTO_REBALANCE_STATUS}; extra master has ${slots}/${expected_slots} slots."
      return 0
    fi

    if [[ "${slots}" -gt 0 && "${AUTO_REBALANCE_DETECTED}" == "false" ]]; then
      AUTO_REBALANCE_DETECTED=true
      AUTO_REBALANCE_STATUS="in_progress"
      AUTO_REBALANCE_START="${now}"
      echo "  Auto-rebalance detected at t=$((now - memtier_start))s with ${slots} slots on extra master."
    fi

    sleep 2
    waited=$((waited + 2))
  done

  AUTO_REBALANCE_END="$(date +%s)"
  if [[ "${AUTO_REBALANCE_DETECTED}" == "true" ]]; then
    AUTO_REBALANCE_DURATION=$((AUTO_REBALANCE_END - AUTO_REBALANCE_START))
    AUTO_REBALANCE_STATUS="partial"
  fi

  echo "  Auto-rebalance ${AUTO_REBALANCE_STATUS}; extra master has ${AUTO_REBALANCE_FINAL_SLOTS}/${expected_slots} slots."
  return 1
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
  local use_atomic="${2:-false}"
  local weight_args=()
  local weight
  local atomic_args=()

  mapfile -t weight_args < <(rebalance_weight_args "${new_master_id}" 1)
  if [[ "${#weight_args[@]}" -lt 2 ]]; then
    echo "ERROR: Not enough masters available for weighted rebalance." >&2
    return 1
  fi
  if [[ "${use_atomic}" == "true" ]]; then
    atomic_args=(--cluster-use-atomic-slot-migration)
  fi

  echo "  ${CURRENT_RESHARD_MODE^} rebalance: equal weights across original masters and extra master ${new_master_id}..."
  for weight in "${weight_args[@]}"; do
    echo "    --cluster-weight ${weight}"
  done

  kubectl exec valkey-0 -n "${NS}" -- \
    valkey-cli --cluster rebalance "${CLUSTER_ENDPOINT}" \
      --cluster-use-empty-primaries \
      "${atomic_args[@]}" \
      --cluster-weight "${weight_args[@]}" \
      --cluster-yes 2>&1
}

weighted_rebalance_off_extra_master() {
  local new_master_id="$1"
  local use_atomic="${2:-false}"
  local weight_args=()
  local weight
  local atomic_args=()

  mapfile -t weight_args < <(rebalance_weight_args "${new_master_id}" 0)
  if [[ "${#weight_args[@]}" -lt 2 ]]; then
    echo "ERROR: Not enough masters available for weighted scale-in rebalance." >&2
    return 1
  fi
  if [[ "${use_atomic}" == "true" ]]; then
    atomic_args=(--cluster-use-atomic-slot-migration)
  fi

  echo "  ${CURRENT_RESHARD_MODE^} rebalance: draining extra master ${new_master_id} with weight 0..."
  for weight in "${weight_args[@]}"; do
    echo "    --cluster-weight ${weight}"
  done

  kubectl exec valkey-0 -n "${NS}" -- \
    valkey-cli --cluster rebalance "${CLUSTER_ENDPOINT}" \
      "${atomic_args[@]}" \
      --cluster-weight "${weight_args[@]}" \
      --cluster-yes 2>&1
}

reset_fallback_rebalance_state() {
  FALLBACK_REBALANCE_USED=false
  FALLBACK_REBALANCE_STATUS="not_needed"
  FALLBACK_REBALANCE_START=0
  FALLBACK_REBALANCE_END=0
  FALLBACK_REBALANCE_DURATION=0
  FALLBACK_REBALANCE_SLOTS_BEFORE=0
  FALLBACK_REBALANCE_FINAL_SLOTS=0
}

move_slots_to_extra_master() {
  local new_master_id="$1"
  local slots_to_move="$2"

  if [[ "${slots_to_move}" -le 0 ]]; then
    echo "  Explicit ${CURRENT_RESHARD_MODE} rebalance: no missing slots to move."
    return 0
  fi

  case "${CURRENT_RESHARD_MODE}" in
    legacy)
      weighted_rebalance_to_extra_master "${new_master_id}" false
      ;;
    atomic)
      weighted_rebalance_to_extra_master "${new_master_id}" true
      ;;
    *)
      echo "ERROR: CURRENT_RESHARD_MODE is not set to legacy or atomic." >&2
      return 1
      ;;
  esac
}

run_fallback_rebalance_if_needed() {
  local new_master_id="$1"
  local expected_slots="$2"
  local max_wait="$3"
  local waited=0 slots missing

  reset_fallback_rebalance_state

  FALLBACK_REBALANCE_SLOTS_BEFORE="$(slot_count_for_node_id "${new_master_id}" || echo 0)"
  FALLBACK_REBALANCE_FINAL_SLOTS="${FALLBACK_REBALANCE_SLOTS_BEFORE}"

  if [[ "${FALLBACK_REBALANCE_SLOTS_BEFORE}" -ge "${expected_slots}" ]] && cluster_is_healthy_now; then
    FALLBACK_REBALANCE_STATUS="already_complete"
    FALLBACK_REBALANCE_START="$(date +%s)"
    FALLBACK_REBALANCE_END="${FALLBACK_REBALANCE_START}"
    FALLBACK_REBALANCE_DURATION=0
    return 0
  fi

  FALLBACK_REBALANCE_USED=true
  FALLBACK_REBALANCE_STATUS="in_progress"
  FALLBACK_REBALANCE_START="$(date +%s)"

  missing=$((expected_slots - FALLBACK_REBALANCE_SLOTS_BEFORE))
  if [[ "${missing}" -lt 0 ]]; then
    missing=0
  fi

  echo "  Explicit ${CURRENT_RESHARD_MODE} rebalance required: extra master has ${FALLBACK_REBALANCE_SLOTS_BEFORE}/${expected_slots} slots."

  if ! move_slots_to_extra_master "${new_master_id}" "${missing}"; then
    FALLBACK_REBALANCE_STATUS="failed"
    FALLBACK_REBALANCE_END="$(date +%s)"
    FALLBACK_REBALANCE_DURATION=$((FALLBACK_REBALANCE_END - FALLBACK_REBALANCE_START))
    FALLBACK_REBALANCE_FINAL_SLOTS="$(slot_count_for_node_id "${new_master_id}" || echo 0)"
    return 1
  fi

  echo "  Waiting for explicit ${CURRENT_RESHARD_MODE} rebalance to settle (max ${max_wait}s)..."
  while [[ "${waited}" -lt "${max_wait}" ]]; do
    slots="$(slot_count_for_node_id "${new_master_id}" || echo 0)"
    FALLBACK_REBALANCE_FINAL_SLOTS="${slots}"

    if [[ "${slots}" -ge "${expected_slots}" ]] && cluster_is_healthy_now; then
      FALLBACK_REBALANCE_STATUS="complete"
      FALLBACK_REBALANCE_END="$(date +%s)"
      FALLBACK_REBALANCE_DURATION=$((FALLBACK_REBALANCE_END - FALLBACK_REBALANCE_START))
      echo "  Explicit ${CURRENT_RESHARD_MODE} rebalance complete; extra master has ${slots}/${expected_slots} slots."
      return 0
    fi

    sleep 5
    waited=$((waited + 5))
  done

  FALLBACK_REBALANCE_END="$(date +%s)"
  FALLBACK_REBALANCE_DURATION=$((FALLBACK_REBALANCE_END - FALLBACK_REBALANCE_START))
  if [[ "${FALLBACK_REBALANCE_FINAL_SLOTS}" -gt "${FALLBACK_REBALANCE_SLOTS_BEFORE}" ]]; then
    FALLBACK_REBALANCE_STATUS="partial"
  else
    FALLBACK_REBALANCE_STATUS="timeout"
  fi

  echo "  Explicit ${CURRENT_RESHARD_MODE} rebalance ${FALLBACK_REBALANCE_STATUS}; extra master has ${FALLBACK_REBALANCE_FINAL_SLOTS}/${expected_slots} slots."
  return 1
}

extra_master_id() {
  local ordinal pod flags

  for ordinal in $(seq "${NEW_NODE_START}" "${NEW_NODE_END}"); do
    pod="valkey-${ordinal}"
    flags="$(node_flags_for_pod "${pod}" || true)"
    if [[ "${flags}" == *master* ]]; then
      node_id_for_pod "${pod}"
      return 0
    fi
  done
}

move_slots_off_extra_master() {
  local new_master_id="$1"
  local slots_on_new
  slots_on_new="$(slot_count_for_node_id "${new_master_id}")"

  if [[ "${slots_on_new}" -le 0 ]]; then
    echo "  No slots found on extra master ${new_master_id}; skipping slot migration."
    return 0
  fi

  case "${CURRENT_RESHARD_MODE}" in
    legacy)
      weighted_rebalance_off_extra_master "${new_master_id}" false
      ;;
    atomic)
      weighted_rebalance_off_extra_master "${new_master_id}" true
      ;;
    *)
      echo "ERROR: CURRENT_RESHARD_MODE is not set to legacy or atomic." >&2
      return 1
      ;;
  esac
}

delete_extra_cluster_nodes() {
  local ordinal pod node_id flags

  for ordinal in $(seq "${NEW_NODE_START}" "${NEW_NODE_END}"); do
    pod="valkey-${ordinal}"
    flags="$(node_flags_for_pod "${pod}" || true)"
    if [[ "${flags}" == *slave* ]]; then
      node_id="$(node_id_for_pod "${pod}" || true)"
      if [[ -n "${node_id}" ]]; then
        echo "  [restore] Removing extra replica ${pod} (${node_id})..."
        kubectl exec valkey-0 -n "${NS}" -- \
          valkey-cli --cluster del-node "${CLUSTER_ENDPOINT}" "${node_id}" 2>&1 || true
        sleep 3
      fi
    fi
  done

  for ordinal in $(seq "${NEW_NODE_START}" "${NEW_NODE_END}"); do
    pod="valkey-${ordinal}"
    flags="$(node_flags_for_pod "${pod}" || true)"
    if [[ "${flags}" == *master* ]]; then
      node_id="$(node_id_for_pod "${pod}" || true)"
      if [[ -n "${node_id}" ]]; then
        echo "  [restore] Removing extra master ${pod} (${node_id})..."
        kubectl exec valkey-0 -n "${NS}" -- \
          valkey-cli --cluster del-node "${CLUSTER_ENDPOINT}" "${node_id}" 2>&1 || true
        sleep 3
      fi
    fi
  done
}

scale_down_to_original_shards() {
  patch_rollout_partition "${ORIGINAL_NODE_COUNT}"
  helm upgrade valkey "${HELM_CHART_PATH}" \
    -n "${NS}" \
    -f "${VALUES_FILE}" \
    --set "cluster.shards=${ORIGINAL_SHARDS}" \
    --set "cluster.autoRebalance.enabled=false" \
    --wait=false 2>&1 || true
  patch_rollout_partition "${ORIGINAL_NODE_COUNT}"

  wait_for_pods_absent "${NEW_NODE_START}" "${NEW_NODE_END}" 300
  wait_for_pods_ready 0 $((ORIGINAL_NODE_COUNT - 1))
  ensure_cluster_clean 300
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

restore_cluster() {
  echo "  [restore] Attempting graceful restore to ${ORIGINAL_SHARDS} shards..."

  patch_rollout_partition "${ORIGINAL_NODE_COUNT}"

  if wait_cluster_healthy 30; then
    local current_masters known_nodes
    current_masters="$(cluster_nodes | awk '/master/ && !/fail/ {count++} END {print count + 0}')"
    known_nodes="$(cluster_info_field cluster_known_nodes || echo 0)"
    if [[ "${current_masters}" -eq "${ORIGINAL_SHARDS}" && "${known_nodes:-0}" -le "${ORIGINAL_NODE_COUNT}" ]] && ! extra_pods_present; then
      patch_rollout_partition 0
      echo "  [restore] Cluster is already at ${ORIGINAL_SHARDS} healthy shards."
      return 0
    fi
  fi

  if ! kubectl get "pod/valkey-${NEW_NODE_START}" -n "${NS}" >/dev/null 2>&1; then
    echo "  [restore] Extra shard pods are missing; scaling back to ${TARGET_SHARDS} shards before slot migration..."
    helm upgrade valkey "${HELM_CHART_PATH}" \
      -n "${NS}" \
      -f "${VALUES_FILE}" \
      --set "cluster.shards=${TARGET_SHARDS}" \
      --set "cluster.autoRebalance.enabled=false" \
      --wait=false
    patch_rollout_partition "${ORIGINAL_NODE_COUNT}"
    wait_for_pods_ready "${NEW_NODE_START}" "${NEW_NODE_END}"
    wait_for_new_nodes_known 180
  fi

  ensure_cluster_clean 300

  local new_master_id
  new_master_id="$(extra_master_id || true)"

  if [[ -z "${new_master_id}" ]]; then
    echo "  [restore] No extra master found in cluster metadata; skipping slot migration."
  else
    move_slots_off_extra_master "${new_master_id}"
    ensure_cluster_clean 180
  fi

  delete_extra_cluster_nodes

  echo "  [restore] Scaling back to ${ORIGINAL_SHARDS} shards via helm..."
  scale_down_to_original_shards
  patch_rollout_partition 0
  sleep 20
  echo "  [restore] Cluster restored."
}

for mode in ${RESHARD_MODES}; do
  validate_reshard_mode "${mode}"
done

for CURRENT_RESHARD_MODE in ${RESHARD_MODES}; do
for i in $(seq 1 "${N}"); do
  POD_NAME="memtier-reshard-${CURRENT_RESHARD_MODE}-${i}"
  OUT_FILE="reshard_${CURRENT_RESHARD_MODE}_run_${i}.json"
  TIMING_FILE="reshard_${CURRENT_RESHARD_MODE}_timing_${i}.json"
  echo ""
  echo "=========================================="
  echo "  Reshard run ${i}/${N} (${CURRENT_RESHARD_MODE})"
  echo "=========================================="

  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found 2>/dev/null || true

  echo "[${i}] Checking for active Chaos Mesh experiments..."
  assert_no_chaos_experiments

  echo "[${i}] Verifying cluster is at ${ORIGINAL_SHARDS} shards..."
  if ! ensure_cluster_clean 60; then
    KNOWN_NODES="$(cluster_info_field cluster_known_nodes || echo 0)"
    CLUSTER_SIZE="$(cluster_info_field cluster_size || echo 0)"

    if [[ "${KNOWN_NODES:-0}" -gt "${ORIGINAL_NODE_COUNT}" || "${CLUSTER_SIZE:-0}" -gt "${ORIGINAL_SHARDS}" ]]; then
      echo "[${i}] Cluster still references extra shard nodes; attempting restore before benchmark..."
      restore_cluster
    fi

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
  ORIGINAL_MASTER_IDS_SNAPSHOT="$(cluster_nodes | awk '/master/ && !/fail/ {print $1}')"

  echo "[${i}] Starting memtier pod for reshard-up (test-time=${TEST_TIME}s)..."
  start_memtier_pod "${POD_NAME}" "${OUT_FILE}"
  MEMTIER_START="$(date +%s)"
  echo "[${i}] Waiting ${STEADY_STATE_WAIT}s for steady state..."
  sleep "${STEADY_STATE_WAIT}"

  echo "[${i}] Guarding existing pods valkey-0..valkey-$((ORIGINAL_NODE_COUNT - 1)) from rolling restart..."
  patch_rollout_partition "${ORIGINAL_NODE_COUNT}"

  echo "[${i}] Scaling up to ${TARGET_SHARDS} shards..."
  SCALE_START="$(date +%s)"
  helm upgrade valkey "${HELM_CHART_PATH}" \
    -n "${NS}" \
    -f "${VALUES_FILE}" \
    --set "cluster.shards=${TARGET_SHARDS}" \
    --set "cluster.autoRebalance.enabled=false" \
    --wait=false
  patch_rollout_partition "${ORIGINAL_NODE_COUNT}"

  echo "[${i}] Waiting for new pods valkey-${NEW_NODE_START}..valkey-${NEW_NODE_END} to be ready..."
  if ! wait_for_pods_ready "${NEW_NODE_START}" "${NEW_NODE_END}" || ! wait_for_new_nodes_known 180; then
    echo "[${i}] ERROR: New shard did not become ready; attempting restore." >&2
    restore_cluster
    exit 1
  fi
  SCALE_END="$(date +%s)"
  echo "[${i}] Scale-up took $((SCALE_END - SCALE_START))s"

  EXTRA_MASTER_ID="$(extra_master_id || true)"
  if [[ -z "${EXTRA_MASTER_ID}" ]]; then
    echo "[${i}] ERROR: Could not find the extra master after scale-up; attempting restore." >&2
    restore_cluster
    exit 1
  fi

  EXPECTED_SLOTS_ON_NEW=$((16384 / TARGET_SHARDS))
  AUTO_TRACE_FILE="reshard_${CURRENT_RESHARD_MODE}_auto_rebalance_${i}.csv"
  echo "[${i}] Running explicit ${CURRENT_RESHARD_MODE} slot migration onto extra master ${EXTRA_MASTER_ID}..."
  reset_auto_rebalance_state
  reset_fallback_rebalance_state
  AUTO_REBALANCE_FINAL_SLOTS="$(slot_count_for_node_id "${EXTRA_MASTER_ID}" || echo 0)"
  {
    echo "timestamp,second,slots_on_extra_master,cluster_state,cluster_slots_ok,cluster_slots_assigned"
    now="$(date +%s)"
    printf "%s,%s,%s,%s,%s,%s\n" \
      "${now}" \
      "$((now - MEMTIER_START))" \
      "${AUTO_REBALANCE_FINAL_SLOTS}" \
      "$(cluster_info_field cluster_state || echo unknown)" \
      "$(cluster_info_field cluster_slots_ok || echo unknown)" \
      "$(cluster_info_field cluster_slots_assigned || echo unknown)"
  } > "${LOCAL_OUT}/${AUTO_TRACE_FILE}"

  if ! ensure_cluster_clean 60; then
    echo "[${i}] WARN: Cluster is not fully clean before explicit ${CURRENT_RESHARD_MODE} migration; trying migration anyway."
  fi

  if ! run_fallback_rebalance_if_needed "${EXTRA_MASTER_ID}" "${EXPECTED_SLOTS_ON_NEW}" "${AUTO_REBALANCE_TIMEOUT}"; then
    echo "[${i}] ERROR: Explicit ${CURRENT_RESHARD_MODE} rebalance did not complete; attempting restore." >&2
    restore_cluster
    exit 1
  fi

  if ! ensure_cluster_clean 180; then
    echo "[${i}] ERROR: Cluster unhealthy after rebalance; attempting restore." >&2
    restore_cluster
    exit 1
  fi

  AUTO_REBALANCE_FINAL_SLOTS="${FALLBACK_REBALANCE_FINAL_SLOTS}"
  echo "[${i}] Auto-rebalance status: ${AUTO_REBALANCE_STATUS}, duration=${AUTO_REBALANCE_DURATION}s"
  if [[ "${FALLBACK_REBALANCE_USED}" == "true" ]]; then
    echo "[${i}] Explicit ${CURRENT_RESHARD_MODE} rebalance status: ${FALLBACK_REBALANCE_STATUS}, duration=${FALLBACK_REBALANCE_DURATION}s, slots=${FALLBACK_REBALANCE_FINAL_SLOTS}/${EXPECTED_SLOTS_ON_NEW}"
  fi

  NEW_MASTERS="$(cluster_nodes | awk '/master/ && !/fail/ {count++} END {print count + 0}')"
  echo "[${i}] Masters after auto-rebalance observation: ${NEW_MASTERS}"

  echo "[${i}] Waiting for reshard-up memtier to finish..."
  if ! finish_memtier_pod "${POD_NAME}" "${OUT_FILE}"; then
    kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found
    restore_cluster
    exit 1
  fi

  OPERATION_END="${AUTO_REBALANCE_END}"
  if [[ "${FALLBACK_REBALANCE_END}" -gt 0 ]]; then
    OPERATION_END="${FALLBACK_REBALANCE_END}"
  fi

  cat > "${LOCAL_OUT}/${TIMING_FILE}" <<EOF
{
  "run": ${i},
  "phase": "up",
  "provider": "selfhosted_valkey",
  "slot_migration_mode": "${CURRENT_RESHARD_MODE}",
  "atomic_slot_migration": $(atomic_mode_json),
  "slot_migration_command": "$(slot_migration_command_name)",
  "scale_start": ${SCALE_START},
  "scale_end": ${SCALE_END},
  "scale_duration_s": $((SCALE_END - SCALE_START)),
  "scale_start_s": $((SCALE_START - MEMTIER_START)),
  "scale_end_s": $((SCALE_END - MEMTIER_START)),
  "auto_rebalance_detected": ${AUTO_REBALANCE_DETECTED},
  "auto_rebalance_status": "${AUTO_REBALANCE_STATUS}",
  "auto_rebalance_start": $(json_number_or_null "${AUTO_REBALANCE_START}"),
  "auto_rebalance_end": $(json_number_or_null "${AUTO_REBALANCE_END}"),
  "auto_rebalance_duration_s": ${AUTO_REBALANCE_DURATION},
  "auto_rebalance_start_s": $(relative_or_null "${AUTO_REBALANCE_START}" "${MEMTIER_START}"),
  "auto_rebalance_end_s": $(relative_or_null "${AUTO_REBALANCE_END}" "${MEMTIER_START}"),
  "auto_rebalance_trace": "${AUTO_TRACE_FILE}",
  "expected_slots_on_new": ${EXPECTED_SLOTS_ON_NEW},
  "slots_on_new_after": ${AUTO_REBALANCE_FINAL_SLOTS},
  "explicit_rebalance_used": ${FALLBACK_REBALANCE_USED},
  "explicit_rebalance_status": "${FALLBACK_REBALANCE_STATUS}",
  "explicit_rebalance_start": $(json_number_or_null "${FALLBACK_REBALANCE_START}"),
  "explicit_rebalance_end": $(json_number_or_null "${FALLBACK_REBALANCE_END}"),
  "explicit_rebalance_duration_s": ${FALLBACK_REBALANCE_DURATION},
  "explicit_rebalance_start_s": $(relative_or_null "${FALLBACK_REBALANCE_START}" "${MEMTIER_START}"),
  "explicit_rebalance_end_s": $(relative_or_null "${FALLBACK_REBALANCE_END}" "${MEMTIER_START}"),
  "explicit_rebalance_slots_before": ${FALLBACK_REBALANCE_SLOTS_BEFORE},
  "explicit_rebalance_slots_after": ${FALLBACK_REBALANCE_FINAL_SLOTS},
  "fallback_rebalance_used": false,
  "fallback_rebalance_status": "not_used",
  "fallback_rebalance_duration_s": 0,
  "rebalance_start": $(json_number_or_null "${FALLBACK_REBALANCE_START}"),
  "rebalance_end": $(json_number_or_null "${FALLBACK_REBALANCE_END}"),
  "rebalance_duration_s": ${FALLBACK_REBALANCE_DURATION},
  "rebalance_start_s": $(relative_or_null "${FALLBACK_REBALANCE_START}" "${MEMTIER_START}"),
  "rebalance_end_s": $(relative_or_null "${FALLBACK_REBALANCE_END}" "${MEMTIER_START}"),
  "operation_start_s": $((SCALE_START - MEMTIER_START)),
  "operation_end_s": $((OPERATION_END - MEMTIER_START)),
  "operation_duration_s": $((OPERATION_END - SCALE_START)),
  "memtier_start": ${MEMTIER_START},
  "original_shards": ${ORIGINAL_SHARDS},
  "target_shards": ${TARGET_SHARDS},
  "masters_after": ${NEW_MASTERS}
}
EOF
  echo "[${i}] Timing data saved to ${LOCAL_OUT}/${TIMING_FILE}"

  echo "[${i}] Cleaning up memtier pod..."
  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found

  DOWN_POD_NAME="memtier-reshard-${CURRENT_RESHARD_MODE}-down-${i}"
  DOWN_OUT_FILE="reshard_${CURRENT_RESHARD_MODE}_down_run_${i}.json"
  DOWN_TIMING_FILE="reshard_${CURRENT_RESHARD_MODE}_down_timing_${i}.json"

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
  if ! ensure_cluster_clean 180; then
    echo "[${i}] ERROR: Cluster unhealthy after reshard-down slot migration; attempting restore." >&2
    kubectl delete pod "${DOWN_POD_NAME}" -n "${NS}" --ignore-not-found
    restore_cluster
    exit 1
  fi

  echo "[${i}] Removing extra cluster nodes under load..."
  DEL_NODE_START="$(date +%s)"
  delete_extra_cluster_nodes
  DEL_NODE_END="$(date +%s)"

  echo "[${i}] Scaling Helm release back to ${ORIGINAL_SHARDS} shards under load..."
  SCALE_DOWN_START="$(date +%s)"
  if ! scale_down_to_original_shards; then
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
  "provider": "selfhosted_valkey",
  "slot_migration_mode": "${CURRENT_RESHARD_MODE}",
  "atomic_slot_migration": $(atomic_mode_json),
  "slot_migration_command": "$(slot_migration_command_name)",
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
  patch_rollout_partition 0

  echo "[${i}] Done. Results: ${LOCAL_OUT}/${OUT_FILE}, ${LOCAL_OUT}/${TIMING_FILE}, ${LOCAL_OUT}/${DOWN_OUT_FILE}, ${LOCAL_OUT}/${DOWN_TIMING_FILE}"
done
done

echo ""
echo "=========================================="
echo "  All ${N} reshard runs complete for modes: ${RESHARD_MODES}."
echo "  Results in: ${LOCAL_OUT}/"
echo "  Each run contains reshard-up and reshard-down memtier/timing files."
echo "  Analyse with:"
echo "    python cli.py reshard --input ${LOCAL_OUT} --output-dir ./plots/reshard"
echo "=========================================="

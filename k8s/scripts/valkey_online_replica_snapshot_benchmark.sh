#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 <dataset_mb_total> [output-dir]

Runs an online Valkey backup/restore benchmark without kubectl cp for backup
artifacts:
  1. create a fresh Valkey cluster,
  2. seed deterministic random data,
  3. BGSAVE on replicas while the source cluster remains online,
  4. validate replica RDB files,
  5. snapshot replica PVC disks,
  6. restore into a fresh cluster by mounting snapshot disks inside Kubernetes,
  7. verify restored data.

Environment:
  N=1
  NS=vk
  RELEASE=valkey
  PROJECT_ID=<gcloud active project>
  LOCATION=europe-central2
  ZONE=europe-central2-a
  CHART_PATH=../valkey-helm/valkey
  VALUES_FILE=k8s/manifests/values-backup-max7gb.yaml
  BACKUP_IMAGE=<LOCATION>-docker.pkg.dev/<PROJECT_ID>/valkey-bench/backup_restore:1
  COPY_IMAGE=busybox:1.36
  RANDOM_DATA=true
  PARALLEL_SHARD_OPS=true
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if ! command -v gcloud >/dev/null 2>&1 && [[ -x "${HOME}/google-cloud-sdk/bin/gcloud" ]]; then
  export PATH="${HOME}/google-cloud-sdk/bin:${PATH}"
fi

DATASET_MB="${1:?Usage: $0 <dataset_mb_total> [output-dir]}"
LOCAL_OUT="${2:-./results/valkey_online_replica_snapshot}"
N="${N:-1}"
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
LOCATION="${LOCATION:-europe-central2}"
ZONE="${ZONE:-europe-central2-a}"
NS="${NS:-vk}"
RELEASE="${RELEASE:-valkey}"
CHART_PATH="${CHART_PATH:-../valkey-helm/valkey}"
VALUES_FILE="${VALUES_FILE:-k8s/manifests/values-backup-max7gb.yaml}"
ARTIFACT_REPO="${ARTIFACT_REPO:-valkey-bench}"
BACKUP_IMAGE="${BACKUP_IMAGE:-${LOCATION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REPO}/backup_restore:1}"
COPY_IMAGE="${COPY_IMAGE:-busybox:1.36}"
RESTORE_DISK_TYPE="${RESTORE_DISK_TYPE:-pd-balanced}"
EXPECTED_SHARDS="${EXPECTED_SHARDS:-3}"
RANDOM_DATA="${RANDOM_DATA:-true}"
PARALLEL_SHARD_OPS="${PARALLEL_SHARD_OPS:-true}"
REMOTE_OUT="/work/results/online_replica_snapshot"
KUBECTL_EXEC_TIMEOUT_SECONDS="${KUBECTL_EXEC_TIMEOUT_SECONDS:-30}"
RDB_CHECK_TIMEOUT_SECONDS="${RDB_CHECK_TIMEOUT_SECONDS:-1800}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON_BIN="$(command -v python3 || command -v python || true)"

if [[ -z "${PYTHON_BIN}" ]]; then
  echo "ERROR: python3 or python is required." >&2
  exit 1
fi

if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "(unset)" ]]; then
  echo "ERROR: Could not determine GCP project. Set PROJECT_ID or run gcloud config set project." >&2
  exit 1
fi

source "${SCRIPT_DIR}/pod_results.sh"

mkdir -p "${LOCAL_OUT}"

RANDOM_DATA_ARG=""
if [[ "${RANDOM_DATA}" == "true" ]]; then
  RANDOM_DATA_ARG="--random-data"
fi

json_string() {
  "${PYTHON_BIN}" -c 'import json, sys; print(json.dumps(sys.argv[1]))' "$1"
}

wait_for_parallel_jobs() {
  local failed=0
  local pid

  for pid in "$@"; do
    if ! wait "${pid}"; then
      failed=1
    fi
  done

  return "${failed}"
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
  local timeout_s="${2:-180}"

  kubectl wait pod/"${pod_name}" -n "${NS}" \
    --for=condition=Ready --timeout="${timeout_s}s"
}

wait_for_no_valkey_pods() {
  local timeout_s="${1:-300}"
  local deadline=$((SECONDS + timeout_s))

  while (( SECONDS < deadline )); do
    local count
    count="$(kubectl get pods -n "${NS}" \
      -l app.kubernetes.io/instance="${RELEASE}",app.kubernetes.io/name=valkey \
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

wait_for_pvcs_bound() {
  local expected="$1"
  local timeout_s="${2:-300}"
  local selector="${3:-app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/name=valkey}"
  local deadline=$((SECONDS + timeout_s))

  while (( SECONDS < deadline )); do
    local bound
    bound="$(kubectl get pvc -n "${NS}" -l "${selector}" \
      --no-headers 2>/dev/null | awk '$2 == "Bound" { c++ } END { print c + 0 }')" || true
    if [[ "${bound}" == "${expected}" ]]; then
      return 0
    fi
    sleep 2
  done

  kubectl get pvc -n "${NS}" || true
  return 1
}

wait_for_single_pvc_bound() {
  local pvc_name="$1"
  local timeout_s="${2:-300}"
  local deadline=$((SECONDS + timeout_s))

  while (( SECONDS < deadline )); do
    local phase
    phase="$(kubectl get pvc "${pvc_name}" -n "${NS}" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    if [[ "${phase}" == "Bound" ]]; then
      return 0
    fi
    sleep 2
  done

  kubectl get pvc "${pvc_name}" -n "${NS}" -o wide || true
  return 1
}

wait_cluster_healthy() {
  local max_wait="${1:-600}"
  local elapsed=0
  echo "  Waiting for cluster to become healthy (max ${max_wait}s)..."
  while [[ ${elapsed} -lt ${max_wait} ]]; do
    local state slots masters
    state="$(kubectl exec "${RELEASE}-0" -c valkey -n "${NS}" -- \
      valkey-cli cluster info 2>/dev/null | grep cluster_state | tr -d '[:space:]')" || true
    slots="$(kubectl exec "${RELEASE}-0" -c valkey -n "${NS}" -- \
      valkey-cli cluster info 2>/dev/null | grep cluster_slots_ok | cut -d: -f2 | tr -d '[:space:]\r')" || true
    masters="$(kubectl exec "${RELEASE}-0" -c valkey -n "${NS}" -- \
      valkey-cli cluster nodes 2>/dev/null | grep master | grep -v fail | wc -l | tr -d '[:space:]')" || true
    if [[ "${state}" == "cluster_state:ok" && "${slots}" == "16384" && "${masters}" -ge "${EXPECTED_SHARDS}" ]]; then
      echo "  Cluster healthy (${masters} masters) after ${elapsed}s"
      return 0
    fi
    sleep 5
    elapsed=$((elapsed + 5))
  done

  echo "ERROR: Cluster not healthy after ${max_wait}s" >&2
  kubectl get pods -n "${NS}" -o wide || true
  kubectl exec "${RELEASE}-0" -c valkey -n "${NS}" -- valkey-cli cluster info || true
  kubectl exec "${RELEASE}-0" -c valkey -n "${NS}" -- valkey-cli cluster nodes || true
  return 1
}

cluster_nodes_raw() {
  kubectl exec "${RELEASE}-0" -c valkey -n "${NS}" -- \
    valkey-cli cluster nodes 2>/dev/null
}

pod_ip_json() {
  kubectl get pods -n "${NS}" \
    -l app.kubernetes.io/instance="${RELEASE}",app.kubernetes.io/name=valkey \
    -o json
}

write_backup_plan() {
  local plan_file="$1"
  local nodes_file="$2"
  local pods_file="$3"

  "${PYTHON_BIN}" - "${nodes_file}" "${pods_file}" "${RELEASE}" "${EXPECTED_SHARDS}" > "${plan_file}" <<'PY'
import json
import sys

nodes_path, pods_path, release, expected_shards = sys.argv[1:5]
expected_shards = int(expected_shards)
nodes_raw = open(nodes_path).read()
pods = json.load(open(pods_path))

ip_to_pod = {}
for item in pods.get("items", []):
    name = item.get("metadata", {}).get("name", "")
    ip = item.get("status", {}).get("podIP")
    if ip:
        ip_to_pod[ip] = name

def endpoint_to_pod(endpoint: str) -> str:
    if "," in endpoint:
        endpoint, hostname = endpoint.split(",", 1)
        pod = hostname.split(".", 1)[0]
        if pod.startswith(f"{release}-"):
            return pod
    endpoint = endpoint.split("@", 1)[0]
    host = endpoint.rsplit(":", 1)[0]
    if host in ip_to_pod:
        return ip_to_pod[host]
    if host.startswith(f"{release}-"):
        return host.split(".", 1)[0]
    return ""

masters = {}
replicas = {}

for line in nodes_raw.splitlines():
    parts = line.split()
    if len(parts) < 8:
        continue
    node_id, endpoint, flags_raw = parts[0], parts[1], parts[2]
    flags = set(flags_raw.split(","))
    if "fail" in flags or "handshake" in flags:
        continue
    pod = endpoint_to_pod(endpoint)
    if not pod:
        continue
    if "master" in flags:
        slots = parts[8:]
        if slots:
            masters[node_id] = {"pod": pod, "slots": slots}
    elif "slave" in flags or "replica" in flags:
        master_id = parts[3]
        replicas.setdefault(master_id, []).append(pod)

rows = []
for master_id, master in sorted(masters.items(), key=lambda item: item[1]["pod"]):
    replica_list = sorted(set(replicas.get(master_id, [])))
    if not replica_list:
        raise SystemExit(f"Master {master['pod']} has no discovered replica")
    rows.append((master_id, master["pod"], replica_list[0], ",".join(master["slots"])))

if len(rows) != expected_shards:
    details = ", ".join(f"{master}->{replica} {slots}" for _, master, replica, slots in rows)
    raise SystemExit(
        f"Expected {expected_shards} shard backups, discovered {len(rows)}: {details}"
    )

for row in rows:
    print("\t".join(row))
PY
}

target_slot_plan() {
  local output_file="$1"
  local nodes_file pods_file
  nodes_file="$(mktemp)"
  pods_file="$(mktemp)"
  cluster_nodes_raw > "${nodes_file}"
  pod_ip_json > "${pods_file}"
  write_backup_plan "${output_file}" "${nodes_file}" "${pods_file}"
  rm -f "${nodes_file}" "${pods_file}"
}

validate_target_slots() {
  local manifest_file="$1"
  local target_plan="$2"

  "${PYTHON_BIN}" - "${manifest_file}" "${target_plan}" <<'PY'
import json
import sys

manifest = json.load(open(sys.argv[1]))
expected = {
    shard["target_master_pod"]: ",".join(shard["slots"])
    for shard in manifest.get("shards", [])
}
actual = {}
with open(sys.argv[2]) as fh:
    for line in fh:
        master_id, master_pod, replica_pod, slots = line.rstrip("\n").split("\t")
        actual[master_pod] = slots

missing = sorted(set(expected) - set(actual))
if missing:
    raise SystemExit(f"Target cluster missing expected masters: {', '.join(missing)}")

mismatched = []
for pod, slots in sorted(expected.items()):
    if actual[pod] != slots:
        mismatched.append(f"{pod}: backup={slots} target={actual[pod]}")

if mismatched:
    raise SystemExit("Target slot layout differs from backup:\n" + "\n".join(mismatched))
PY
}

pvc_disk_name() {
  local pvc_name="$1"
  local pv_name
  pv_name="$(kubectl get pvc "${pvc_name}" -n "${NS}" -o jsonpath='{.spec.volumeName}')"
  kubectl get pv "${pv_name}" -o jsonpath='{.spec.csi.volumeHandle}' \
    | awk -F/ '{print $NF}'
}

pvc_storage_request() {
  local pvc_name="$1"
  kubectl get pvc "${pvc_name}" -n "${NS}" -o jsonpath='{.spec.resources.requests.storage}'
}

pvc_storage_class() {
  local pvc_name="$1"
  kubectl get pvc "${pvc_name}" -n "${NS}" -o jsonpath='{.spec.storageClassName}'
}

assert_replica_synced() {
  local replica_pod="$1"
  local replication
  replication="$(timeout "${KUBECTL_EXEC_TIMEOUT_SECONDS}s" \
    kubectl exec "${replica_pod}" -c valkey -n "${NS}" -- \
      valkey-cli info replication 2>/dev/null)" || true

  grep -q '^role:slave' <<<"${replication}" || {
    echo "ERROR: ${replica_pod} is not a replica." >&2
    return 1
  }
  grep -q '^master_link_status:up' <<<"${replication}" || {
    echo "ERROR: ${replica_pod} master link is not up." >&2
    return 1
  }
  grep -q '^master_sync_in_progress:0' <<<"${replication}" || {
    echo "ERROR: ${replica_pod} is still syncing." >&2
    return 1
  }
}

trigger_replica_bgsaves() {
  local plan_file="$1"
  local start end
  start="$(date +%s)"

  if [[ "${PARALLEL_SHARD_OPS}" == "true" ]]; then
    local bgsave_pids=()

    while IFS=$'\t' read -r master_id master_pod replica_pod slots; do
      (
        echo "  Checking replica ${replica_pod} for master ${master_pod}..."
        assert_replica_synced "${replica_pod}"
        echo "  BGSAVE ${replica_pod} (${slots})"
        timeout "${KUBECTL_EXEC_TIMEOUT_SECONDS}s" \
          kubectl exec "${replica_pod}" -c valkey -n "${NS}" -- valkey-cli bgsave >/dev/null || true
      ) &
      bgsave_pids+=("$!")
    done < "${plan_file}"

    wait_for_parallel_jobs "${bgsave_pids[@]}"
  else
    while IFS=$'\t' read -r master_id master_pod replica_pod slots; do
      echo "  Checking replica ${replica_pod} for master ${master_pod}..."
      assert_replica_synced "${replica_pod}"
      echo "  BGSAVE ${replica_pod} (${slots})"
      timeout "${KUBECTL_EXEC_TIMEOUT_SECONDS}s" \
        kubectl exec "${replica_pod}" -c valkey -n "${NS}" -- valkey-cli bgsave >/dev/null || true
    done < "${plan_file}"
  fi

  sleep 3
  local max_wait=900
  local waited=0
  local all_done=false
  while [[ "${all_done}" != "true" && ${waited} -lt ${max_wait} ]]; do
    all_done=true
    while IFS=$'\t' read -r master_id master_pod replica_pod slots; do
      local persistence
      persistence="$(timeout "${KUBECTL_EXEC_TIMEOUT_SECONDS}s" \
        kubectl exec "${replica_pod}" -c valkey -n "${NS}" -- \
          valkey-cli info persistence 2>/dev/null)" || true
      if grep -q '^rdb_bgsave_in_progress:1' <<<"${persistence}"; then
        all_done=false
        break
      fi
      if ! grep -q '^rdb_last_bgsave_status:ok' <<<"${persistence}"; then
        echo "ERROR: ${replica_pod} reports failed BGSAVE." >&2
        return 1
      fi
    done < "${plan_file}"

    if [[ "${all_done}" != "true" ]]; then
      echo "    Replica BGSAVE still running after ${waited}s..."
      sleep 5
      waited=$((waited + 5))
    fi
  done

  [[ "${all_done}" == "true" ]] || {
    echo "ERROR: Replica BGSAVE did not finish within ${max_wait}s." >&2
    return 1
  }

  end="$(date +%s)"
  echo "${start} ${end} $((end - start))"
}

validate_replica_rdbs() {
  local plan_file="$1"
  local start end
  start="$(date +%s)"

  if [[ "${PARALLEL_SHARD_OPS}" == "true" ]]; then
    local validate_pids=()

    while IFS=$'\t' read -r master_id master_pod replica_pod slots; do
      (
        echo "  Validating RDB on ${replica_pod} for ${master_pod} (${slots})"
        timeout "${RDB_CHECK_TIMEOUT_SECONDS}s" \
          kubectl exec "${replica_pod}" -c valkey -n "${NS}" -- \
            valkey-check-rdb /data/dump.rdb >/dev/null
      ) &
      validate_pids+=("$!")
    done < "${plan_file}"

    wait_for_parallel_jobs "${validate_pids[@]}"
  else
    while IFS=$'\t' read -r master_id master_pod replica_pod slots; do
      echo "  Validating RDB on ${replica_pod} for ${master_pod} (${slots})"
      timeout "${RDB_CHECK_TIMEOUT_SECONDS}s" \
        kubectl exec "${replica_pod}" -c valkey -n "${NS}" -- \
          valkey-check-rdb /data/dump.rdb >/dev/null
    done < "${plan_file}"
  fi

  end="$(date +%s)"
  echo "${start} ${end} $((end - start))"
}

write_online_snapshot_manifest() {
  local manifest_file="$1"
  local backup_dir="$2"
  local plan_file="$3"
  local disks_file="$4"
  local backup_start="$5"
  local bgsave_start="$6"
  local bgsave_end="$7"
  local bgsave_duration="$8"
  local validate_start="$9"
  local validate_end="${10}"
  local validate_duration="${11}"
  local snapshot_start="${12}"
  local snapshot_end="${13}"

  "${PYTHON_BIN}" - "${manifest_file}" "${backup_dir}" "${plan_file}" "${disks_file}" \
    "${PROJECT_ID}" "${LOCATION}" "${ZONE}" "${NS}" "${RELEASE}" \
    "${backup_start}" "${bgsave_start}" "${bgsave_end}" "${bgsave_duration}" \
    "${validate_start}" "${validate_end}" "${validate_duration}" \
    "${snapshot_start}" "${snapshot_end}" <<'PY'
import json
import os
import sys
import time

(
    manifest_file, backup_dir, plan_file, disks_file, project_id, location, zone,
    namespace, release, backup_start, bgsave_start, bgsave_end, bgsave_duration,
    validate_start, validate_end, validate_duration, snapshot_start, snapshot_end,
) = sys.argv[1:]

disk_items = {}
with open(disks_file) as fh:
    for line in fh:
        if line.strip():
            item = json.loads(line)
            disk_items[item["target_master_pod"]] = item

shards = []
with open(plan_file) as fh:
    for line in fh:
        master_id, master_pod, replica_pod, slots = line.rstrip("\n").split("\t")
        item = disk_items[master_pod]
        shards.append({
            "master_id": master_id,
            "target_master_pod": master_pod,
            "source_replica_pod": replica_pod,
            "slots": slots.split(","),
            **item,
        })

now = int(time.time())
doc = {
    "variant": "valkey_online_replica_pvc_snapshot",
    "project_id": project_id,
    "location": location,
    "zone": zone,
    "namespace": namespace,
    "release": release,
    "created_at_epoch_s": now,
    "backup_start": int(backup_start),
    "backup_end": now,
    "backup_duration_s": now - int(backup_start),
    "bgsave_start": int(bgsave_start),
    "bgsave_end": int(bgsave_end),
    "bgsave_duration_s": int(bgsave_duration),
    "rdb_validation_start": int(validate_start),
    "rdb_validation_end": int(validate_end),
    "rdb_validation_duration_s": int(validate_duration),
    "snapshot_create_start": int(snapshot_start),
    "snapshot_create_end": int(snapshot_end),
    "snapshot_create_duration_s": int(snapshot_end) - int(snapshot_start),
    "shards": shards,
}

with open(manifest_file, "w") as fh:
    json.dump(doc, fh, indent=2)
    fh.write("\n")
PY
}

create_online_replica_snapshot_backup() {
  local run_idx="$1"
  local backup_dir="$2"
  local snapshot_prefix="$3"
  local manifest_file="${backup_dir}/online_replica_snapshot_manifest.json"
  local timing_file="${backup_dir}/online_replica_snapshot_timing.json"
  local plan_file="${backup_dir}/online_replica_snapshot_plan.tsv"
  local nodes_file="${backup_dir}/cluster_nodes.txt"
  local pods_file="${backup_dir}/pods.json"
  local disks_file="${backup_dir}/snapshot_disks.jsonl"

  mkdir -p "${backup_dir}"
  : > "${disks_file}"

  echo "==> Online replica PVC snapshot backup"
  echo "NS=${NS}"
  echo "RELEASE=${RELEASE}"
  echo "OUTPUT=${backup_dir}"
  echo "SNAPSHOT_PREFIX=${snapshot_prefix}"

  local backup_start
  backup_start="$(date +%s)"
  wait_cluster_healthy 600

  cluster_nodes_raw > "${nodes_file}"
  pod_ip_json > "${pods_file}"
  write_backup_plan "${plan_file}" "${nodes_file}" "${pods_file}"

  echo "Backup plan:"
  column -t -s $'\t' "${plan_file}" || cat "${plan_file}"

  local bgsave_output bgsave_start bgsave_end bgsave_duration
  bgsave_output="$(trigger_replica_bgsaves "${plan_file}")"
  bgsave_start="$(echo "${bgsave_output}" | tail -1 | awk '{print $1}')"
  bgsave_end="$(echo "${bgsave_output}" | tail -1 | awk '{print $2}')"
  bgsave_duration="$(echo "${bgsave_output}" | tail -1 | awk '{print $3}')"

  local validate_output validate_start validate_end validate_duration
  validate_output="$(validate_replica_rdbs "${plan_file}")"
  validate_start="$(echo "${validate_output}" | tail -1 | awk '{print $1}')"
  validate_end="$(echo "${validate_output}" | tail -1 | awk '{print $2}')"
  validate_duration="$(echo "${validate_output}" | tail -1 | awk '{print $3}')"

  local snapshot_start snapshot_end
  snapshot_start="$(date +%s)"
  if [[ "${PARALLEL_SHARD_OPS}" == "true" ]]; then
    local snapshot_tmp
    local snapshot_pids=()
    snapshot_tmp="$(mktemp -d)"

    while IFS=$'\t' read -r master_id master_pod replica_pod slots; do
      (
        master_ordinal="${master_pod##*-}"
        replica_ordinal="${replica_pod##*-}"
        source_pvc="valkey-data-${RELEASE}-${replica_ordinal}"
        target_pvc="valkey-data-${RELEASE}-${master_ordinal}"
        source_disk="$(pvc_disk_name "${source_pvc}")"
        storage_request="$(pvc_storage_request "${source_pvc}")"
        storage_class="$(pvc_storage_class "${source_pvc}")"
        snapshot="${snapshot_prefix}-${master_ordinal}-from-${replica_ordinal}"

        echo "  Snapshot ${source_pvc} (${source_disk}) -> ${snapshot}"
        one_start="$(date +%s)"
        gcloud compute snapshots create "${snapshot}" \
          --project="${PROJECT_ID}" \
          --source-disk="${source_disk}" \
          --source-disk-zone="${ZONE}" \
          --storage-location="${LOCATION}" \
          --quiet
        one_end="$(date +%s)"

        "${PYTHON_BIN}" -c '
import json, sys
print(json.dumps({
  "target_master_pod": sys.argv[1],
  "source_replica_pod": sys.argv[2],
  "source_pvc": sys.argv[3],
  "target_pvc": sys.argv[4],
  "source_disk": sys.argv[5],
  "snapshot": sys.argv[6],
  "storage_request": sys.argv[7],
  "storage_class": sys.argv[8],
  "snapshot_duration_s": int(sys.argv[9]) - int(sys.argv[10]),
}))
' "${master_pod}" "${replica_pod}" "${source_pvc}" "${target_pvc}" "${source_disk}" "${snapshot}" \
          "${storage_request}" "${storage_class}" "${one_end}" "${one_start}" > "${snapshot_tmp}/${master_ordinal}.json"
      ) &
      snapshot_pids+=("$!")
    done < "${plan_file}"

    wait_for_parallel_jobs "${snapshot_pids[@]}"
    for f in "${snapshot_tmp}"/*.json; do
      cat "${f}" >> "${disks_file}"
    done
    rm -rf "${snapshot_tmp}"
  else
    while IFS=$'\t' read -r master_id master_pod replica_pod slots; do
      local master_ordinal replica_ordinal source_pvc target_pvc source_disk snapshot storage_request storage_class one_start one_end
      master_ordinal="${master_pod##*-}"
      replica_ordinal="${replica_pod##*-}"
      source_pvc="valkey-data-${RELEASE}-${replica_ordinal}"
      target_pvc="valkey-data-${RELEASE}-${master_ordinal}"
      source_disk="$(pvc_disk_name "${source_pvc}")"
      storage_request="$(pvc_storage_request "${source_pvc}")"
      storage_class="$(pvc_storage_class "${source_pvc}")"
      snapshot="${snapshot_prefix}-${master_ordinal}-from-${replica_ordinal}"

      echo "  Snapshot ${source_pvc} (${source_disk}) -> ${snapshot}"
      one_start="$(date +%s)"
      gcloud compute snapshots create "${snapshot}" \
        --project="${PROJECT_ID}" \
        --source-disk="${source_disk}" \
        --source-disk-zone="${ZONE}" \
        --storage-location="${LOCATION}" \
        --quiet
      one_end="$(date +%s)"

      "${PYTHON_BIN}" -c '
import json, sys
print(json.dumps({
  "target_master_pod": sys.argv[1],
  "source_replica_pod": sys.argv[2],
  "source_pvc": sys.argv[3],
  "target_pvc": sys.argv[4],
  "source_disk": sys.argv[5],
  "snapshot": sys.argv[6],
  "storage_request": sys.argv[7],
  "storage_class": sys.argv[8],
  "snapshot_duration_s": int(sys.argv[9]) - int(sys.argv[10]),
}))
' "${master_pod}" "${replica_pod}" "${source_pvc}" "${target_pvc}" "${source_disk}" "${snapshot}" \
        "${storage_request}" "${storage_class}" "${one_end}" "${one_start}" >> "${disks_file}"
    done < "${plan_file}"
  fi
  snapshot_end="$(date +%s)"

  write_online_snapshot_manifest "${manifest_file}" "${backup_dir}" "${plan_file}" "${disks_file}" \
    "${backup_start}" "${bgsave_start}" "${bgsave_end}" "${bgsave_duration}" \
    "${validate_start}" "${validate_end}" "${validate_duration}" \
    "${snapshot_start}" "${snapshot_end}"

  local backup_end
  backup_end="$(date +%s)"
  cat > "${timing_file}" <<EOF
{
  "variant": "valkey_online_replica_pvc_snapshot_backup",
  "project_id": $(json_string "${PROJECT_ID}"),
  "location": $(json_string "${LOCATION}"),
  "zone": $(json_string "${ZONE}"),
  "namespace": $(json_string "${NS}"),
  "release": $(json_string "${RELEASE}"),
  "manifest": $(json_string "${manifest_file}"),
  "backup_start": ${backup_start},
  "bgsave_start": ${bgsave_start},
  "bgsave_end": ${bgsave_end},
  "bgsave_duration_s": ${bgsave_duration},
  "rdb_validation_start": ${validate_start},
  "rdb_validation_end": ${validate_end},
  "rdb_validation_duration_s": ${validate_duration},
  "snapshot_create_start": ${snapshot_start},
  "snapshot_create_end": ${snapshot_end},
  "snapshot_create_duration_s": $((snapshot_end - snapshot_start)),
  "backup_end": ${backup_end},
  "backup_duration_s": $((backup_end - backup_start))
}
EOF

  echo ""
  echo "Backup manifest: ${manifest_file}"
  echo "Backup timing: ${timing_file}"
}

delete_release_and_pvcs() {
  helm uninstall "${RELEASE}" -n "${NS}" --ignore-not-found || true
  wait_for_no_valkey_pods 300 || true
  kubectl delete pvc -n "${NS}" \
    -l app.kubernetes.io/instance="${RELEASE}",app.kubernetes.io/name=valkey \
    --ignore-not-found --wait=true || true
}

install_fresh_cluster() {
  echo "  Installing fresh Valkey cluster..."
  delete_release_and_pvcs
  helm install "${RELEASE}" "${CHART_PATH}" -n "${NS}" -f "${VALUES_FILE}" --wait=false
  kubectl rollout status sts/"${RELEASE}" -n "${NS}" --timeout=900s
  wait_cluster_healthy 600
}

seed_data() {
  local run_idx="$1"
  local run_dir="$2"
  local run_id="$3"
  local report_file="$4"
  local pod_name="online-snapshot-seed-${run_idx}"

  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl run "${pod_name}" -n "${NS}" \
    --image="${BACKUP_IMAGE}" \
    --image-pull-policy=Always \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_OUT}'
      python /work/backup_restore_seed.py \
        --mode seed \
        --host '${RELEASE}.${NS}.svc.cluster.local' \
        --port 6379 \
        --target-mb '${DATASET_MB}' \
        --run-id '${run_id}' \
        ${RANDOM_DATA_ARG} \
        --output '${REMOTE_OUT}/${report_file}'
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  wait_for_command_pod "${pod_name}" 14400
  kubectl cp "${NS}/${pod_name}:${REMOTE_OUT}/${report_file}" "${run_dir}/${report_file}"
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

verify_data() {
  local run_idx="$1"
  local run_dir="$2"
  local seed_report="$3"
  local verify_report="$4"
  local pod_name="online-snapshot-verify-${run_idx}"

  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl run "${pod_name}" -n "${NS}" \
    --image="${BACKUP_IMAGE}" \
    --image-pull-policy=Always \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_OUT}'
      while [ ! -f '${REMOTE_OUT}/${seed_report}' ]; do sleep 1; done
      python /work/backup_restore_seed.py \
        --mode verify \
        --host '${RELEASE}.${NS}.svc.cluster.local' \
        --port 6379 \
        --seed-report '${REMOTE_OUT}/${seed_report}' \
        --output '${REMOTE_OUT}/${verify_report}'
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  wait_for_pod_ready "${pod_name}" 120
  kubectl cp "${run_dir}/${seed_report}" "${NS}/${pod_name}:${REMOTE_OUT}/${seed_report}"
  wait_for_command_pod "${pod_name}" 7200
  kubectl cp "${NS}/${pod_name}:${REMOTE_OUT}/${verify_report}" "${run_dir}/${verify_report}"
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

manifest_items_base64() {
  local manifest_file="$1"
  "${PYTHON_BIN}" -c '
import base64, json, sys
for item in json.load(open(sys.argv[1]))["shards"]:
    print(base64.b64encode(json.dumps(item).encode()).decode())
' "${manifest_file}"
}

item_field() {
  local encoded="$1"
  local field="$2"
  "${PYTHON_BIN}" -c '
import base64, json, sys
item = json.loads(base64.b64decode(sys.argv[1]))
value = item.get(sys.argv[2], "")
if isinstance(value, list):
    print(",".join(value))
else:
    print(value)
' "${encoded}" "${field}"
}

create_source_snapshot_pvc() {
  local ordinal="$1"
  local snapshot="$2"
  local storage_request="$3"
  local storage_class="$4"
  local disk_name="$5"
  local pv_name="$6"
  local pvc_name="$7"

  gcloud compute disks create "${disk_name}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --source-snapshot="${snapshot}" \
    --type="${RESTORE_DISK_TYPE}" \
    --quiet

  kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolume
metadata:
  name: ${pv_name}
  labels:
    app.kubernetes.io/instance: ${RELEASE}
    app.kubernetes.io/name: online-replica-snapshot-source
spec:
  capacity:
    storage: ${storage_request}
  accessModes:
    - ReadWriteOnce
  persistentVolumeReclaimPolicy: Retain
  storageClassName: ${storage_class}
  claimRef:
    namespace: ${NS}
    name: ${pvc_name}
  csi:
    driver: pd.csi.storage.gke.io
    volumeHandle: projects/${PROJECT_ID}/zones/${ZONE}/disks/${disk_name}
    fsType: ext4
  nodeAffinity:
    required:
      nodeSelectorTerms:
        - matchExpressions:
            - key: topology.gke.io/zone
              operator: In
              values:
                - ${ZONE}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${pvc_name}
  namespace: ${NS}
  labels:
    app.kubernetes.io/instance: ${RELEASE}
    app.kubernetes.io/name: online-replica-snapshot-source
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: ${storage_request}
  storageClassName: ${storage_class}
  volumeName: ${pv_name}
EOF
}

run_pvc_copy_pod() {
  local pod_name="$1"
  local source_pvc="$2"
  local target_pvc="$3"

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
      image: ${COPY_IMAGE}
      command:
        - sh
        - -c
        - |
          set -eu
          rm -f /target/dump.rdb /target/appendonly.aof
          rm -rf /target/appendonlydir
          cp /source/dump.rdb /target/dump.rdb
          chmod 644 /target/dump.rdb
          echo 0 > ${POD_EXIT_CODE_FILE}
          touch ${POD_DONE_FILE}
          sleep ${POD_HOLD_SECONDS}
      volumeMounts:
        - name: source
          mountPath: /source
          readOnly: true
        - name: target
          mountPath: /target
  volumes:
    - name: source
      persistentVolumeClaim:
        claimName: ${source_pvc}
        readOnly: true
    - name: target
      persistentVolumeClaim:
        claimName: ${target_pvc}
EOF

  wait_for_command_pod "${pod_name}" 3600
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

clear_replica_pvc() {
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
    - name: clear
      image: ${COPY_IMAGE}
      command:
        - sh
        - -c
        - |
          set -eu
          rm -f /data/dump.rdb /data/appendonly.aof
          rm -rf /data/appendonlydir
          echo 0 > ${POD_EXIT_CODE_FILE}
          touch ${POD_DONE_FILE}
          sleep ${POD_HOLD_SECONDS}
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: ${claim_name}
EOF

  wait_for_command_pod "${pod_name}" 900
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

cleanup_source_snapshot_pvc() {
  local pv_name="$1"
  local pvc_name="$2"
  local disk_name="$3"

  kubectl delete pvc "${pvc_name}" -n "${NS}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  kubectl delete pv "${pv_name}" --ignore-not-found >/dev/null 2>&1 || true
  gcloud compute disks delete "${disk_name}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --quiet >/dev/null 2>&1 || true
}

restore_online_replica_snapshots() {
  local manifest_file="$1"
  local local_out="$2"
  local restore_disk_prefix="$3"
  local timing_file="${local_out}/online_replica_snapshot_restore_timing.json"
  local target_plan="${local_out}/target_plan.tsv"
  local restored_jsonl="${local_out}/restored_snapshot_sources.jsonl"

  mkdir -p "${local_out}"
  : > "${restored_jsonl}"

  echo "==> Online replica PVC snapshot restore"
  echo "MANIFEST=${manifest_file}"
  echo "RESTORE_DISK_PREFIX=${restore_disk_prefix}"

  local restore_start
  restore_start="$(date +%s)"

  echo "  Creating fresh target cluster..."
  install_fresh_cluster
  local fresh_cluster_ready
  fresh_cluster_ready="$(date +%s)"

  target_slot_plan "${target_plan}"
  validate_target_slots "${manifest_file}" "${target_plan}"

  echo "  Scaling target cluster down before RDB injection..."
  local scale_down_start
  scale_down_start="$(date +%s)"
  kubectl scale sts/"${RELEASE}" -n "${NS}" --replicas=0
  wait_for_no_valkey_pods 300
  local scale_down_end
  scale_down_end="$(date +%s)"

  echo "  Creating temporary PVCs from replica snapshots..."
  local disk_create_start
  disk_create_start="$(date +%s)"
  if [[ "${PARALLEL_SHARD_OPS}" == "true" ]]; then
    local restore_tmp
    local source_create_pids=()
    restore_tmp="$(mktemp -d)"

    for encoded in $(manifest_items_base64 "${manifest_file}"); do
      (
        master_pod="$(item_field "${encoded}" target_master_pod)"
        master_ordinal="${master_pod##*-}"
        snapshot="$(item_field "${encoded}" snapshot)"
        storage_request="$(item_field "${encoded}" storage_request)"
        storage_class="$(item_field "${encoded}" storage_class)"
        source_pvc="${RELEASE}-online-snapshot-source-${master_ordinal}"
        source_pv="${restore_disk_prefix}-source-pv-${master_ordinal}"
        source_disk="${restore_disk_prefix}-source-${master_ordinal}"

        echo "  Creating ${source_disk} from ${snapshot}"
        one_start="$(date +%s)"
        create_source_snapshot_pvc "${master_ordinal}" "${snapshot}" "${storage_request}" "${storage_class}" \
          "${source_disk}" "${source_pv}" "${source_pvc}"
        wait_for_single_pvc_bound "${source_pvc}" 300
        one_end="$(date +%s)"

        "${PYTHON_BIN}" -c '
import json, sys
print(json.dumps({
  "master_pod": sys.argv[1],
  "source_snapshot": sys.argv[2],
  "source_pvc": sys.argv[3],
  "source_pv": sys.argv[4],
  "source_disk": sys.argv[5],
  "disk_create_duration_s": int(sys.argv[6]) - int(sys.argv[7]),
}))
' "${master_pod}" "${snapshot}" "${source_pvc}" "${source_pv}" "${source_disk}" "${one_end}" "${one_start}" > "${restore_tmp}/${master_ordinal}.json"
      ) &
      source_create_pids+=("$!")
    done

    wait_for_parallel_jobs "${source_create_pids[@]}"
    for f in "${restore_tmp}"/*.json; do
      cat "${f}" >> "${restored_jsonl}"
    done
    rm -rf "${restore_tmp}"
  else
    for encoded in $(manifest_items_base64 "${manifest_file}"); do
      local master_pod master_ordinal snapshot storage_request storage_class source_pvc source_pv source_disk
      master_pod="$(item_field "${encoded}" target_master_pod)"
      master_ordinal="${master_pod##*-}"
      snapshot="$(item_field "${encoded}" snapshot)"
      storage_request="$(item_field "${encoded}" storage_request)"
      storage_class="$(item_field "${encoded}" storage_class)"
      source_pvc="${RELEASE}-online-snapshot-source-${master_ordinal}"
      source_pv="${restore_disk_prefix}-source-pv-${master_ordinal}"
      source_disk="${restore_disk_prefix}-source-${master_ordinal}"

      echo "  Creating ${source_disk} from ${snapshot}"
      local one_start one_end
      one_start="$(date +%s)"
      create_source_snapshot_pvc "${master_ordinal}" "${snapshot}" "${storage_request}" "${storage_class}" \
        "${source_disk}" "${source_pv}" "${source_pvc}"
      wait_for_single_pvc_bound "${source_pvc}" 300
      one_end="$(date +%s)"

      "${PYTHON_BIN}" -c '
import json, sys
print(json.dumps({
  "master_pod": sys.argv[1],
  "source_snapshot": sys.argv[2],
  "source_pvc": sys.argv[3],
  "source_pv": sys.argv[4],
  "source_disk": sys.argv[5],
  "disk_create_duration_s": int(sys.argv[6]) - int(sys.argv[7]),
}))
' "${master_pod}" "${snapshot}" "${source_pvc}" "${source_pv}" "${source_disk}" "${one_end}" "${one_start}" >> "${restored_jsonl}"
    done
  fi
  local disk_create_end
  disk_create_end="$(date +%s)"

  echo "  Copying RDB files inside Kubernetes from snapshot PVCs to target master PVCs..."
  local inject_start
  inject_start="$(date +%s)"
  if [[ "${PARALLEL_SHARD_OPS}" == "true" ]]; then
    local copy_pids=()
    for encoded in $(manifest_items_base64 "${manifest_file}"); do
      (
        master_pod="$(item_field "${encoded}" target_master_pod)"
        master_ordinal="${master_pod##*-}"
        source_pvc="${RELEASE}-online-snapshot-source-${master_ordinal}"
        target_pvc="valkey-data-${RELEASE}-${master_ordinal}"
        copy_pod="online-snapshot-copy-${master_ordinal}"
        echo "  Copying ${source_pvc}:/dump.rdb -> ${target_pvc}:/dump.rdb"
        run_pvc_copy_pod "${copy_pod}" "${source_pvc}" "${target_pvc}"
      ) &
      copy_pids+=("$!")
    done
    wait_for_parallel_jobs "${copy_pids[@]}"
  else
    for encoded in $(manifest_items_base64 "${manifest_file}"); do
      local master_pod master_ordinal source_pvc target_pvc copy_pod
      master_pod="$(item_field "${encoded}" target_master_pod)"
      master_ordinal="${master_pod##*-}"
      source_pvc="${RELEASE}-online-snapshot-source-${master_ordinal}"
      target_pvc="valkey-data-${RELEASE}-${master_ordinal}"
      copy_pod="online-snapshot-copy-${master_ordinal}"
      echo "  Copying ${source_pvc}:/dump.rdb -> ${target_pvc}:/dump.rdb"
      run_pvc_copy_pod "${copy_pod}" "${source_pvc}" "${target_pvc}"
    done
  fi
  local inject_end
  inject_end="$(date +%s)"

  echo "  Clearing target replica PVCs before resync..."
  local clear_start
  clear_start="$(date +%s)"
  if [[ "${PARALLEL_SHARD_OPS}" == "true" ]]; then
    local clear_pids=()
    while IFS=$'\t' read -r master_id master_pod replica_pod slots; do
      (
        replica_ordinal="${replica_pod##*-}"
        claim_name="valkey-data-${RELEASE}-${replica_ordinal}"
        clear_pod="online-snapshot-clear-${replica_ordinal}"
        echo "  Clearing ${claim_name} (${replica_pod})"
        clear_replica_pvc "${clear_pod}" "${claim_name}"
      ) &
      clear_pids+=("$!")
    done < "${target_plan}"
    wait_for_parallel_jobs "${clear_pids[@]}"
  else
    while IFS=$'\t' read -r master_id master_pod replica_pod slots; do
      local replica_ordinal claim_name clear_pod
      replica_ordinal="${replica_pod##*-}"
      claim_name="valkey-data-${RELEASE}-${replica_ordinal}"
      clear_pod="online-snapshot-clear-${replica_ordinal}"
      echo "  Clearing ${claim_name} (${replica_pod})"
      clear_replica_pvc "${clear_pod}" "${claim_name}"
    done < "${target_plan}"
  fi
  local clear_end
  clear_end="$(date +%s)"

  echo "  Cleaning temporary source PVCs/disks..."
  local temp_cleanup_start
  temp_cleanup_start="$(date +%s)"
  while read -r line; do
    [[ -z "${line}" ]] && continue
    local pv pvc disk
    pv="$(LINE="${line}" "${PYTHON_BIN}" -c 'import json, os; print(json.loads(os.environ["LINE"])["source_pv"])')"
    pvc="$(LINE="${line}" "${PYTHON_BIN}" -c 'import json, os; print(json.loads(os.environ["LINE"])["source_pvc"])')"
    disk="$(LINE="${line}" "${PYTHON_BIN}" -c 'import json, os; print(json.loads(os.environ["LINE"])["source_disk"])')"
    cleanup_source_snapshot_pvc "${pv}" "${pvc}" "${disk}"
  done < "${restored_jsonl}"
  local temp_cleanup_end
  temp_cleanup_end="$(date +%s)"

  echo "  Starting restored cluster..."
  local cluster_start
  cluster_start="$(date +%s)"
  kubectl scale sts/"${RELEASE}" -n "${NS}" --replicas=6
  kubectl rollout status sts/"${RELEASE}" -n "${NS}" --timeout=900s
  local pods_ready
  pods_ready="$(date +%s)"
  wait_cluster_healthy 900
  local cluster_ready
  cluster_ready="$(date +%s)"

  cat > "${timing_file}" <<EOF
{
  "variant": "valkey_online_replica_pvc_snapshot_restore",
  "project_id": $(json_string "${PROJECT_ID}"),
  "zone": $(json_string "${ZONE}"),
  "namespace": $(json_string "${NS}"),
  "release": $(json_string "${RELEASE}"),
  "manifest": $(json_string "${manifest_file}"),
  "restore_disk_prefix": $(json_string "${restore_disk_prefix}"),
  "restore_start": ${restore_start},
  "fresh_cluster_ready_ts": ${fresh_cluster_ready},
  "fresh_cluster_create_duration_s": $((fresh_cluster_ready - restore_start)),
  "scale_down_start": ${scale_down_start},
  "scale_down_end": ${scale_down_end},
  "scale_down_duration_s": $((scale_down_end - scale_down_start)),
  "source_disk_create_start": ${disk_create_start},
  "source_disk_create_end": ${disk_create_end},
  "source_disk_create_duration_s": $((disk_create_end - disk_create_start)),
  "rdb_incluster_copy_start": ${inject_start},
  "rdb_incluster_copy_end": ${inject_end},
  "rdb_incluster_copy_duration_s": $((inject_end - inject_start)),
  "replica_clear_start": ${clear_start},
  "replica_clear_end": ${clear_end},
  "replica_clear_duration_s": $((clear_end - clear_start)),
  "temp_source_cleanup_start": ${temp_cleanup_start},
  "temp_source_cleanup_end": ${temp_cleanup_end},
  "temp_source_cleanup_duration_s": $((temp_cleanup_end - temp_cleanup_start)),
  "cluster_start": ${cluster_start},
  "pods_ready_ts": ${pods_ready},
  "pod_recreate_duration_s": $((pods_ready - cluster_start)),
  "ready_ts": ${cluster_ready},
  "cluster_recovery_after_pods_s": $((cluster_ready - pods_ready)),
  "restore_duration_s": $((cluster_ready - restore_start))
}
EOF

  echo ""
  echo "Restore timing: ${timing_file}"
}

write_run_summary() {
  local run_idx="$1"
  local run_dir="$2"
  local run_id="$3"
  local seed_report="$4"
  local verify_report="$5"
  local backup_manifest="${run_dir}/backup/online_replica_snapshot_manifest.json"
  local backup_timing="${run_dir}/backup/online_replica_snapshot_timing.json"
  local restore_timing="${run_dir}/restore/online_replica_snapshot_restore_timing.json"
  local summary_file="${run_dir}/online_replica_snapshot_benchmark_run_${run_idx}.json"

  "${PYTHON_BIN}" - "${run_idx}" "${run_id}" "${DATASET_MB}" \
    "${run_dir}/${seed_report}" "${run_dir}/${verify_report}" \
    "${backup_manifest}" "${backup_timing}" "${restore_timing}" "${summary_file}" <<'PY'
import json
import sys

(
    run_idx, run_id, dataset_mb, seed_path, verify_path, manifest_path,
    backup_timing_path, restore_timing_path, summary_path,
) = sys.argv[1:]

seed = json.load(open(seed_path))
verify = json.load(open(verify_path))
manifest = json.load(open(manifest_path))
backup = json.load(open(backup_timing_path))
restore = json.load(open(restore_timing_path))

doc = {
    "variant": "valkey_online_replica_pvc_snapshot_benchmark",
    "run": int(run_idx),
    "run_id": run_id,
    "dataset_mb_total": int(dataset_mb),
    "random_data": bool(seed.get("random_data", False)),
    "seed_duration_s": seed.get("seed_duration_s"),
    "seed_keys": seed.get("written_keys"),
    "backup_duration_s": backup.get("backup_duration_s"),
    "bgsave_duration_s": backup.get("bgsave_duration_s"),
    "rdb_validation_duration_s": backup.get("rdb_validation_duration_s"),
    "snapshot_create_duration_s": backup.get("snapshot_create_duration_s"),
    "fresh_cluster_create_duration_s": restore.get("fresh_cluster_create_duration_s"),
    "source_disk_create_duration_s": restore.get("source_disk_create_duration_s"),
    "rdb_incluster_copy_duration_s": restore.get("rdb_incluster_copy_duration_s"),
    "replica_clear_duration_s": restore.get("replica_clear_duration_s"),
    "temp_source_cleanup_duration_s": restore.get("temp_source_cleanup_duration_s"),
    "pod_recreate_duration_s": restore.get("pod_recreate_duration_s"),
    "cluster_recovery_after_pods_s": restore.get("cluster_recovery_after_pods_s"),
    "restore_duration_s": restore.get("restore_duration_s"),
    "verify_duration_s": verify.get("verify_duration_s"),
    "verify_sample_size": verify.get("sample_size"),
    "integrity_ok": verify.get("integrity_ok"),
    "backup_manifest": manifest_path,
    "backup_timing": backup_timing_path,
    "restore_timing": restore_timing_path,
}

with open(summary_path, "w") as fh:
    json.dump(doc, fh, indent=2)
    fh.write("\n")
PY
}

write_summary_csv() {
  local output="${LOCAL_OUT}/online_replica_snapshot_summary.csv"
  "${PYTHON_BIN}" - "${LOCAL_OUT}" "${output}" <<'PY'
import csv
import glob
import json
import os
import sys

root, output = sys.argv[1:]
paths = sorted(glob.glob(os.path.join(root, "run_*", "online_replica_snapshot_benchmark_run_*.json")))
fields = [
    "run",
    "dataset_mb_total",
    "random_data",
    "seed_duration_s",
    "backup_duration_s",
    "bgsave_duration_s",
    "rdb_validation_duration_s",
    "snapshot_create_duration_s",
    "fresh_cluster_create_duration_s",
    "source_disk_create_duration_s",
    "rdb_incluster_copy_duration_s",
    "replica_clear_duration_s",
    "temp_source_cleanup_duration_s",
    "pod_recreate_duration_s",
    "cluster_recovery_after_pods_s",
    "restore_duration_s",
    "verify_duration_s",
    "integrity_ok",
]

with open(output, "w", newline="") as fh:
    writer = csv.DictWriter(fh, fieldnames=fields)
    writer.writeheader()
    for path in paths:
        row = json.load(open(path))
        writer.writerow({field: row.get(field) for field in fields})

print(output)
PY
}

echo "==> Valkey online replica PVC snapshot benchmark"
echo "PROJECT_ID=${PROJECT_ID}"
echo "LOCATION=${LOCATION}"
echo "ZONE=${ZONE}"
echo "NS=${NS}"
echo "RELEASE=${RELEASE}"
echo "DATASET_MB=${DATASET_MB}"
echo "N=${N}"
echo "RANDOM_DATA=${RANDOM_DATA}"
echo "PARALLEL_SHARD_OPS=${PARALLEL_SHARD_OPS}"
echo "VALUES_FILE=${VALUES_FILE}"
echo "BACKUP_IMAGE=${BACKUP_IMAGE}"
echo "COPY_IMAGE=${COPY_IMAGE}"
echo "OUTPUT=${LOCAL_OUT}"

for i in $(seq 1 "${N}"); do
  echo ""
  echo "=========================================="
  echo "  Online replica PVC snapshot run ${i}/${N} (${DATASET_MB} MB total)"
  echo "=========================================="

  RUN_DIR="${LOCAL_OUT}/run_${i}"
  BACKUP_DIR="${RUN_DIR}/backup"
  RESTORE_DIR="${RUN_DIR}/restore"
  mkdir -p "${RUN_DIR}" "${BACKUP_DIR}" "${RESTORE_DIR}"

  RUN_ID="online-replica-snapshot-${DATASET_MB}mb-${i}-$(date +%s)"
  SEED_REPORT="seed_report_online_snapshot_${DATASET_MB}_${i}.json"
  VERIFY_REPORT="verify_report_online_snapshot_${DATASET_MB}_${i}.json"

  install_fresh_cluster

  echo "[${i}] Seeding ${DATASET_MB} MB..."
  seed_data "${i}" "${RUN_DIR}" "${RUN_ID}" "${SEED_REPORT}"

  echo "[${i}] Creating online replica PVC snapshots..."
  create_online_replica_snapshot_backup "${i}" "${BACKUP_DIR}" \
    "${RELEASE}-online-replica-snap-${i}-$(date +%Y%m%d-%H%M%S)"

  echo "[${i}] Restoring from online replica PVC snapshots..."
  restore_online_replica_snapshots \
    "${BACKUP_DIR}/online_replica_snapshot_manifest.json" \
    "${RESTORE_DIR}" \
    "${RELEASE}-online-restore-${i}-$(date +%Y%m%d-%H%M%S)"

  echo "[${i}] Verifying restored data..."
  verify_data "${i}" "${RUN_DIR}" "${SEED_REPORT}" "${VERIFY_REPORT}"

  write_run_summary "${i}" "${RUN_DIR}" "${RUN_ID}" "${SEED_REPORT}" "${VERIFY_REPORT}"
  echo "[${i}] Summary: ${RUN_DIR}/online_replica_snapshot_benchmark_run_${i}.json"
done

CSV_PATH="$(write_summary_csv)"

echo ""
echo "=========================================="
echo "  All ${N} online replica PVC snapshot runs complete."
echo "  Results in: ${LOCAL_OUT}/"
echo "  CSV summary: ${CSV_PATH}"
echo "=========================================="

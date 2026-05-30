#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage:
  $0 backup [output-dir]
  $0 restore <manifest.json> [output-dir]

Creates an online RDB backup from Valkey Cluster replicas and restores it into
a fresh cluster. The source cluster stays online during backup. Restore is
offline and destructive for the target Helm release/PVCs.

Environment:
  NS=vk
  RELEASE=valkey
  CHART_PATH=../valkey-helm/valkey
  VALUES_FILE=k8s/manifests/values.yaml
  COPY_IMAGE=busybox:1.36
EOF
}

MODE="${1:-}"
if [[ -z "${MODE}" || "${MODE}" == "-h" || "${MODE}" == "--help" ]]; then
  usage
  exit 0
fi

NS="${NS:-vk}"
RELEASE="${RELEASE:-valkey}"
CHART_PATH="${CHART_PATH:-../valkey-helm/valkey}"
VALUES_FILE="${VALUES_FILE:-k8s/manifests/values.yaml}"
COPY_IMAGE="${COPY_IMAGE:-busybox:1.36}"
EXPECTED_SHARDS="${EXPECTED_SHARDS:-3}"
KUBECTL_EXEC_TIMEOUT_SECONDS="${KUBECTL_EXEC_TIMEOUT_SECONDS:-30}"
KUBECTL_CP_TIMEOUT_SECONDS="${KUBECTL_CP_TIMEOUT_SECONDS:-1800}"
RDB_CHECK_TIMEOUT_SECONDS="${RDB_CHECK_TIMEOUT_SECONDS:-1800}"
COPY_RETRIES="${COPY_RETRIES:-3}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON_BIN="$(command -v python3 || command -v python || true)"

if [[ -z "${PYTHON_BIN}" ]]; then
  echo "ERROR: python3 or python is required." >&2
  exit 1
fi

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

wait_for_pvcs() {
  local expected="$1"
  local timeout_s="${2:-300}"
  local deadline=$((SECONDS + timeout_s))

  while (( SECONDS < deadline )); do
    local ready
    ready="$(kubectl get pvc -n "${NS}" \
      -l app.kubernetes.io/instance="${RELEASE}",app.kubernetes.io/name=valkey \
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

local_file_size() {
  local file="$1"
  "${PYTHON_BIN}" -c 'import os, sys; print(os.path.getsize(sys.argv[1]))' "${file}"
}

valkey_pod_file_size() {
  local pod_name="$1"
  local file="$2"

  timeout "${KUBECTL_EXEC_TIMEOUT_SECONDS}s" \
    kubectl exec "${pod_name}" -c valkey -n "${NS}" -- \
      sh -c "wc -c < '${file}'" 2>/dev/null | tr -d '[:space:]'
}

copy_pod_file_size() {
  local pod_name="$1"
  local file="$2"

  timeout "${KUBECTL_EXEC_TIMEOUT_SECONDS}s" \
    kubectl exec "${pod_name}" -n "${NS}" -- \
      sh -c "wc -c < '${file}'" 2>/dev/null | tr -d '[:space:]'
}

copy_from_valkey_pod() {
  local pod_name="$1"
  local remote_file="$2"
  local local_file="$3"
  local expected_size
  expected_size="$(valkey_pod_file_size "${pod_name}" "${remote_file}")"

  if [[ -z "${expected_size}" || "${expected_size}" == "0" ]]; then
    echo "ERROR: Could not determine remote size for ${pod_name}:${remote_file}" >&2
    return 1
  fi

  for attempt in $(seq 1 "${COPY_RETRIES}"); do
    echo "    Copy attempt ${attempt}/${COPY_RETRIES}, expected ${expected_size} bytes" >&2
    rm -f "${local_file}"
    if timeout "${KUBECTL_CP_TIMEOUT_SECONDS}s" \
      kubectl cp -c valkey "${NS}/${pod_name}:${remote_file}" "${local_file}"; then
      local actual_size
      actual_size="$(local_file_size "${local_file}")"
      if [[ "${actual_size}" == "${expected_size}" ]]; then
        return 0
      fi
      echo "    Size mismatch after copy: got ${actual_size}, expected ${expected_size}" >&2
    else
      echo "    kubectl cp failed on attempt ${attempt}" >&2
    fi
  done

  echo "ERROR: Failed to copy ${pod_name}:${remote_file} after ${COPY_RETRIES} attempts." >&2
  return 1
}

copy_to_pvc_pod() {
  local local_file="$1"
  local pod_name="$2"
  local remote_file="$3"
  local expected_size
  expected_size="$(local_file_size "${local_file}")"

  for attempt in $(seq 1 "${COPY_RETRIES}"); do
    echo "    Copy attempt ${attempt}/${COPY_RETRIES}, expected ${expected_size} bytes" >&2
    kubectl exec "${pod_name}" -n "${NS}" -- sh -c "rm -f '${remote_file}'"
    if timeout "${KUBECTL_CP_TIMEOUT_SECONDS}s" \
      kubectl cp "${local_file}" "${NS}/${pod_name}:${remote_file}"; then
      local actual_size
      actual_size="$(copy_pod_file_size "${pod_name}" "${remote_file}")"
      if [[ "${actual_size}" == "${expected_size}" ]]; then
        return 0
      fi
      echo "    Size mismatch after copy: got ${actual_size:-unknown}, expected ${expected_size}" >&2
    else
      echo "    kubectl cp failed on attempt ${attempt}" >&2
    fi
  done

  echo "ERROR: Failed to copy ${local_file} into ${pod_name}:${remote_file} after ${COPY_RETRIES} attempts." >&2
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

if not rows:
    raise SystemExit("No master/replica mapping discovered")
if len(rows) != expected_shards:
    details = ", ".join(f"{master}->{replica} {slots}" for _, master, replica, slots in rows)
    raise SystemExit(
        f"Expected {expected_shards} shard backups, discovered {len(rows)}: {details}"
    )

for row in rows:
    print("\t".join(row))
PY
}

assert_replica_synced() {
  local replica_pod="$1"

  local replication
  replication="$(timeout "${KUBECTL_EXEC_TIMEOUT_SECONDS}s" \
    kubectl exec "${replica_pod}" -c valkey -n "${NS}" -- \
    valkey-cli info replication 2>/dev/null)" || true

  if ! grep -q '^role:slave' <<<"${replication}"; then
    echo "ERROR: ${replica_pod} is not a replica." >&2
    return 1
  fi
  if ! grep -q '^master_link_status:up' <<<"${replication}"; then
    echo "ERROR: ${replica_pod} master link is not up." >&2
    return 1
  fi
  if ! grep -q '^master_sync_in_progress:0' <<<"${replication}"; then
    echo "ERROR: ${replica_pod} is still syncing." >&2
    return 1
  fi
}

trigger_replica_bgsaves() {
  local plan_file="$1"

  local save_start
  save_start="$(date +%s)"

  while IFS=$'\t' read -r master_id master_pod replica_pod slots; do
    echo "  Checking replica ${replica_pod} for master ${master_pod}..." >&2
    assert_replica_synced "${replica_pod}"
    echo "  BGSAVE ${replica_pod} (${slots})" >&2
    timeout "${KUBECTL_EXEC_TIMEOUT_SECONDS}s" \
      kubectl exec "${replica_pod}" -c valkey -n "${NS}" -- valkey-cli bgsave >/dev/null || true
  done < "${plan_file}"

  local max_wait=900
  local waited=0
  local all_done=false
  sleep 3
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
      echo "    Replica BGSAVE still running after ${waited}s..." >&2
      sleep 5
      waited=$((waited + 5))
    fi
  done

  if [[ "${all_done}" != "true" ]]; then
    echo "ERROR: Replica BGSAVE did not finish within ${max_wait}s." >&2
    return 1
  fi

  local save_end
  save_end="$(date +%s)"
  echo "${save_start} ${save_end} $((save_end - save_start))"
}

validate_replica_rdbs() {
  local plan_file="$1"

  local validate_start
  validate_start="$(date +%s)"

  while IFS=$'\t' read -r master_id master_pod replica_pod slots; do
    echo "  Validating RDB on ${replica_pod} for ${master_pod} (${slots})" >&2
    timeout "${RDB_CHECK_TIMEOUT_SECONDS}s" \
      kubectl exec "${replica_pod}" -c valkey -n "${NS}" -- \
        valkey-check-rdb /data/dump.rdb >/dev/null
  done < "${plan_file}"

  local validate_end
  validate_end="$(date +%s)"
  echo "${validate_start} ${validate_end} $((validate_end - validate_start))"
}

copy_replica_rdbs() {
  local plan_file="$1"
  local backup_dir="$2"

  local copy_start
  copy_start="$(date +%s)"

  while IFS=$'\t' read -r master_id master_pod replica_pod slots; do
    local file="${backup_dir}/${master_pod}.from-${replica_pod}.dump.rdb"
    echo "  Copying ${replica_pod}:/data/dump.rdb -> ${file}" >&2
    copy_from_valkey_pod "${replica_pod}" "/data/dump.rdb" "${file}"
  done < "${plan_file}"

  local copy_end
  copy_end="$(date +%s)"
  echo "${copy_start} ${copy_end} $((copy_end - copy_start))"
}

write_backup_manifest() {
  local manifest_file="$1"
  local backup_dir="$2"
  local plan_file="$3"
  local save_start="$4"
  local save_end="$5"
  local save_duration="$6"
  local copy_start="$7"
  local copy_end="$8"
  local copy_duration="$9"
  local validate_start="${10}"
  local validate_end="${11}"
  local validate_duration="${12}"
  local backup_start="${13}"

  "${PYTHON_BIN}" - "${manifest_file}" "${backup_dir}" "${plan_file}" \
    "${NS}" "${RELEASE}" "${VALUES_FILE}" "${CHART_PATH}" \
    "${save_start}" "${save_end}" "${save_duration}" \
    "${copy_start}" "${copy_end}" "${copy_duration}" \
    "${validate_start}" "${validate_end}" "${validate_duration}" \
    "${backup_start}" <<'PY'
import json
import os
import sys
import time

(
    manifest_file, backup_dir, plan_file, namespace, release, values_file,
    chart_path, save_start, save_end, save_duration, copy_start, copy_end,
    copy_duration, validate_start, validate_end, validate_duration, backup_start,
) = sys.argv[1:]

backup_dir_abs = os.path.abspath(backup_dir)
shards = []
with open(plan_file) as fh:
    for line in fh:
        master_id, master_pod, replica_pod, slots = line.rstrip("\n").split("\t")
        name = f"{master_pod}.from-{replica_pod}.dump.rdb"
        path = os.path.join(backup_dir_abs, name)
        shards.append({
            "master_id": master_id,
            "master_pod": master_pod,
            "replica_pod": replica_pod,
            "slots": slots.split(","),
            "rdb_file": path,
            "rdb_size_bytes": os.path.getsize(path),
        })

now = int(time.time())
doc = {
    "variant": "replica_rdb_online_backup",
    "created_at_epoch_s": now,
    "namespace": namespace,
    "release": release,
    "values_file": values_file,
    "chart_path": chart_path,
    "backup_start": int(backup_start),
    "backup_end": now,
    "backup_duration_s": now - int(backup_start),
    "save_start": int(save_start),
    "save_end": int(save_end),
    "save_duration_s": int(save_duration),
    "backup_copy_start": int(copy_start),
    "backup_copy_end": int(copy_end),
    "backup_copy_duration_s": int(copy_duration),
    "rdb_validation_start": int(validate_start),
    "rdb_validation_end": int(validate_end),
    "rdb_validation_duration_s": int(validate_duration),
    "shards": shards,
}
with open(manifest_file, "w") as fh:
    json.dump(doc, fh, indent=2)
    fh.write("\n")
PY
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
      image: ${COPY_IMAGE}
      command: ["sh", "-c", "sleep 3600"]
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

target_slot_plan() {
  local output_file="$1"
  local nodes_file
  local pods_file
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
    shard["master_pod"]: ",".join(shard["slots"])
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

restore_master_rdbs() {
  local manifest_file="$1"

  "${PYTHON_BIN}" - "${manifest_file}" "${RELEASE}" <<'PY' | while IFS=$'\t' read -r master_pod claim_name rdb_file; do
import json
import sys

manifest = json.load(open(sys.argv[1]))
release = sys.argv[2]
for shard in manifest.get("shards", []):
    master_pod = shard["master_pod"]
    ordinal = master_pod.rsplit("-", 1)[-1]
    claim_name = f"valkey-data-{release}-{ordinal}"
    print(f"{master_pod}\t{claim_name}\t{shard['rdb_file']}")
PY
    local ordinal="${master_pod##*-}"
    local copy_pod="replica-rdb-restore-${ordinal}"
    echo "  Injecting ${rdb_file} into ${claim_name} (${master_pod})"
    create_pvc_copy_pod "${copy_pod}" "${claim_name}"
    kubectl exec "${copy_pod}" -n "${NS}" -- sh -c "rm -f /data/dump.rdb /data/appendonly.aof; rm -rf /data/appendonlydir"
    copy_to_pvc_pod "${rdb_file}" "${copy_pod}" "/data/dump.rdb"
    kubectl exec "${copy_pod}" -n "${NS}" -- sh -c "chmod 644 /data/dump.rdb"
    kubectl delete pod "${copy_pod}" -n "${NS}" --ignore-not-found >/dev/null
  done
}

clear_replica_rdbs() {
  local target_plan="$1"

  while IFS=$'\t' read -r master_id master_pod replica_pod slots; do
    local ordinal="${replica_pod##*-}"
    local claim_name="valkey-data-${RELEASE}-${ordinal}"
    local copy_pod="replica-rdb-clear-${ordinal}"
    echo "  Clearing local RDB/AOF from replica PVC ${claim_name} (${replica_pod})"
    create_pvc_copy_pod "${copy_pod}" "${claim_name}"
    kubectl exec "${copy_pod}" -n "${NS}" -- sh -c "rm -f /data/dump.rdb /data/appendonly.aof; rm -rf /data/appendonlydir"
    kubectl delete pod "${copy_pod}" -n "${NS}" --ignore-not-found >/dev/null
  done < "${target_plan}"
}

write_restore_timing() {
  local output_file="$1"
  local manifest_file="$2"
  local restore_start="$3"
  local target_ready="$4"
  local scale_down_start="$5"
  local scale_down_end="$6"
  local inject_start="$7"
  local inject_end="$8"
  local pods_ready="$9"
  local cluster_ready="${10}"

  cat > "${output_file}" <<EOF
{
  "variant": "replica_rdb_restore",
  "manifest": "${manifest_file}",
  "restore_start": ${restore_start},
  "fresh_cluster_ready_ts": ${target_ready},
  "scale_down_start": ${scale_down_start},
  "scale_down_end": ${scale_down_end},
  "scale_down_duration_s": $((scale_down_end - scale_down_start)),
  "rdb_injection_start": ${inject_start},
  "rdb_injection_end": ${inject_end},
  "rdb_injection_duration_s": $((inject_end - inject_start)),
  "pods_ready_ts": ${pods_ready},
  "pod_recreate_duration_s": $((pods_ready - inject_end)),
  "ready_ts": ${cluster_ready},
  "cluster_recovery_after_pods_s": $((cluster_ready - pods_ready)),
  "restore_duration_s": $((cluster_ready - restore_start))
}
EOF
}

run_backup() {
  local local_out="${1:-./results/valkey_replica_rdb_backup}"
  local stamp
  stamp="$(date +%Y%m%d-%H%M%S)"
  local backup_dir="${local_out}/replica_rdb_${stamp}"
  local manifest_file="${backup_dir}/replica_rdb_manifest.json"
  local plan_file="${backup_dir}/replica_rdb_plan.tsv"
  local nodes_file="${backup_dir}/cluster_nodes.txt"
  local pods_file="${backup_dir}/pods.json"

  mkdir -p "${backup_dir}"
  echo "==> Online replica RDB backup"
  echo "NS=${NS}"
  echo "RELEASE=${RELEASE}"
  echo "OUTPUT=${backup_dir}"

  local backup_start
  backup_start="$(date +%s)"
  wait_cluster_healthy 300
  cluster_nodes_raw > "${nodes_file}"
  pod_ip_json > "${pods_file}"
  write_backup_plan "${plan_file}" "${nodes_file}" "${pods_file}"

  echo "Backup plan:"
  column -t -s $'\t' "${plan_file}" || cat "${plan_file}"

  local save_output
  save_output="$(trigger_replica_bgsaves "${plan_file}")"
  local save_start save_end save_duration
  save_start="$(echo "${save_output}" | tail -1 | awk '{print $1}')"
  save_end="$(echo "${save_output}" | tail -1 | awk '{print $2}')"
  save_duration="$(echo "${save_output}" | tail -1 | awk '{print $3}')"

  local validate_output
  validate_output="$(validate_replica_rdbs "${plan_file}")"
  local validate_start validate_end validate_duration
  validate_start="$(echo "${validate_output}" | tail -1 | awk '{print $1}')"
  validate_end="$(echo "${validate_output}" | tail -1 | awk '{print $2}')"
  validate_duration="$(echo "${validate_output}" | tail -1 | awk '{print $3}')"

  local copy_output
  copy_output="$(copy_replica_rdbs "${plan_file}" "${backup_dir}")"
  local copy_start copy_end copy_duration
  copy_start="$(echo "${copy_output}" | tail -1 | awk '{print $1}')"
  copy_end="$(echo "${copy_output}" | tail -1 | awk '{print $2}')"
  copy_duration="$(echo "${copy_output}" | tail -1 | awk '{print $3}')"

  write_backup_manifest "${manifest_file}" "${backup_dir}" "${plan_file}" \
    "${save_start}" "${save_end}" "${save_duration}" \
    "${copy_start}" "${copy_end}" "${copy_duration}" \
    "${validate_start}" "${validate_end}" "${validate_duration}" \
    "${backup_start}"

  echo ""
  echo "Backup manifest: ${manifest_file}"
  echo "Source cluster was not stopped."
}

run_restore() {
  local manifest_file="${1:?Usage: $0 restore <manifest.json> [output-dir]}"
  local local_out="${2:-./results/valkey_replica_rdb_restore}"
  local timing_file="${local_out}/replica_rdb_restore_timing.json"
  local target_plan="${local_out}/target_plan.tsv"

  mkdir -p "${local_out}"
  echo "==> Offline replica RDB restore"
  echo "NS=${NS}"
  echo "RELEASE=${RELEASE}"
  echo "MANIFEST=${manifest_file}"
  echo "VALUES_FILE=${VALUES_FILE}"

  local restore_start
  restore_start="$(date +%s)"

  echo "  Removing existing Helm release and PVCs if present..."
  helm uninstall "${RELEASE}" -n "${NS}" --ignore-not-found
  kubectl delete pvc -n "${NS}" \
    -l app.kubernetes.io/instance="${RELEASE}",app.kubernetes.io/name=valkey \
    --ignore-not-found --wait=true

  echo "  Installing fresh target cluster..."
  helm install "${RELEASE}" "${CHART_PATH}" -n "${NS}" -f "${VALUES_FILE}" --wait=false
  wait_for_pvcs 6 300
  kubectl rollout status sts/"${RELEASE}" -n "${NS}" --timeout=900s
  wait_cluster_healthy 600
  local target_ready
  target_ready="$(date +%s)"

  target_slot_plan "${target_plan}"
  validate_target_slots "${manifest_file}" "${target_plan}"

  echo "  Scaling target cluster down before RDB injection..."
  local scale_down_start
  scale_down_start="$(date +%s)"
  kubectl scale sts/"${RELEASE}" -n "${NS}" --replicas=0
  wait_for_no_valkey_pods 300
  local scale_down_end
  scale_down_end="$(date +%s)"

  echo "  Injecting RDB files into target master PVCs..."
  local inject_start
  inject_start="$(date +%s)"
  restore_master_rdbs "${manifest_file}"
  clear_replica_rdbs "${target_plan}"
  local inject_end
  inject_end="$(date +%s)"

  echo "  Starting restored cluster..."
  kubectl scale sts/"${RELEASE}" -n "${NS}" --replicas=6
  kubectl rollout status sts/"${RELEASE}" -n "${NS}" --timeout=900s
  local pods_ready
  pods_ready="$(date +%s)"
  wait_cluster_healthy 900
  local cluster_ready
  cluster_ready="$(date +%s)"

  write_restore_timing "${timing_file}" "${manifest_file}" \
    "${restore_start}" "${target_ready}" \
    "${scale_down_start}" "${scale_down_end}" \
    "${inject_start}" "${inject_end}" \
    "${pods_ready}" "${cluster_ready}"

  echo ""
  echo "Restore timing: ${timing_file}"
}

case "${MODE}" in
  backup)
    run_backup "${2:-./results/valkey_replica_rdb_backup}"
    ;;
  restore)
    run_restore "${2:-}" "${3:-./results/valkey_replica_rdb_restore}"
    ;;
  *)
    usage
    exit 1
    ;;
esac

#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 [output-dir]

Creates crash-consistent GCE Persistent Disk snapshots for all Valkey PVCs and
writes a manifest plus timing JSON. By default the StatefulSet is scaled to 0
before snapshots to make the on-disk state quiescent.

Defaults:
  PROJECT_ID=<gcloud active project>
  LOCATION=europe-central2
  ZONE=europe-central2-a
  NS=vk
  RELEASE=valkey
  SCALE_DOWN_FOR_SNAPSHOT=true
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

LOCAL_OUT="${1:-./results/valkey_pvc_snapshot_backup}"
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
LOCATION="${LOCATION:-europe-central2}"
ZONE="${ZONE:-europe-central2-a}"
NS="${NS:-vk}"
RELEASE="${RELEASE:-valkey}"
SCALE_DOWN_FOR_SNAPSHOT="${SCALE_DOWN_FOR_SNAPSHOT:-true}"
SNAPSHOT_PREFIX="${SNAPSHOT_PREFIX:-${RELEASE}-pvc-backup-$(date +%Y%m%d-%H%M%S)}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

mkdir -p "${LOCAL_OUT}"

PYTHON_BIN="$(command -v python3 || command -v python || true)"
if [[ -z "${PYTHON_BIN}" ]]; then
  echo "ERROR: python3 or python is required." >&2
  exit 1
fi

if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "(unset)" ]]; then
  echo "ERROR: Could not determine GCP project. Set PROJECT_ID or run gcloud config set project." >&2
  exit 1
fi

json_string() {
  "${PYTHON_BIN}" -c 'import json, sys; print(json.dumps(sys.argv[1]))' "$1"
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
      return 0
    fi
    sleep 5
    elapsed=$((elapsed + 5))
  done
  echo "ERROR: Cluster not healthy after ${max_wait}s" >&2
  return 1
}

trigger_bgsave_all() {
  local start end
  start="$(date +%s)"
  echo "  Running CLUSTER SAVECONFIG..."
  kubectl exec "${RELEASE}-0" -c valkey -n "${NS}" -- \
    valkey-cli --cluster call "${RELEASE}-0.${RELEASE}-headless.${NS}.svc.cluster.local:6379" cluster saveconfig >/dev/null || true

  echo "  Running BGSAVE on all nodes..."
  kubectl exec "${RELEASE}-0" -c valkey -n "${NS}" -- \
    valkey-cli --cluster call "${RELEASE}-0.${RELEASE}-headless.${NS}.svc.cluster.local:6379" bgsave >/dev/null || true

  sleep 5
  local elapsed=0
  local max_wait=900
  while [[ ${elapsed} -lt ${max_wait} ]]; do
    local in_progress
    in_progress="$(kubectl exec "${RELEASE}-0" -c valkey -n "${NS}" -- \
      valkey-cli --cluster call "${RELEASE}-0.${RELEASE}-headless.${NS}.svc.cluster.local:6379" info persistence 2>/dev/null \
      | grep rdb_bgsave_in_progress:1 || true)"
    if [[ -z "${in_progress}" ]]; then
      end="$(date +%s)"
      echo "${start} ${end} $((end - start))"
      return 0
    fi
    echo "  BGSAVE still running after ${elapsed}s..."
    sleep 5
    elapsed=$((elapsed + 5))
  done

  echo "ERROR: BGSAVE did not finish after ${max_wait}s" >&2
  return 1
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

pvc_access_modes_json() {
  local pvc_name="$1"
  kubectl get pvc "${pvc_name}" -n "${NS}" -o jsonpath='{.spec.accessModes}' \
    | "${PYTHON_BIN}" -c 'import json, sys; print(json.dumps(sys.stdin.read().split()))'
}

echo "==> Valkey PVC snapshot backup"
echo "PROJECT_ID=${PROJECT_ID}"
echo "LOCATION=${LOCATION}"
echo "ZONE=${ZONE}"
echo "NS=${NS}"
echo "RELEASE=${RELEASE}"
echo "SNAPSHOT_PREFIX=${SNAPSHOT_PREFIX}"

BACKUP_START="$(date +%s)"
BGSAVE_START=0
BGSAVE_END=0
BGSAVE_DURATION=0
SCALE_DOWN_START=0
SCALE_DOWN_END=0
SCALE_DOWN_DURATION=0

wait_cluster_healthy 300

BGSAVE_OUTPUT="$(trigger_bgsave_all)"
BGSAVE_START="$(echo "${BGSAVE_OUTPUT}" | tail -1 | awk '{print $1}')"
BGSAVE_END="$(echo "${BGSAVE_OUTPUT}" | tail -1 | awk '{print $2}')"
BGSAVE_DURATION="$(echo "${BGSAVE_OUTPUT}" | tail -1 | awk '{print $3}')"

if [[ "${SCALE_DOWN_FOR_SNAPSHOT}" == "true" ]]; then
  echo "  Scaling ${RELEASE} StatefulSet to 0 for quiescent snapshots..."
  SCALE_DOWN_START="$(date +%s)"
  kubectl scale sts/"${RELEASE}" -n "${NS}" --replicas=0
  kubectl rollout status sts/"${RELEASE}" -n "${NS}" --timeout=600s || true
  while kubectl get pod -n "${NS}" -l app.kubernetes.io/instance="${RELEASE}",app.kubernetes.io/name=valkey --no-headers 2>/dev/null | grep -q .; do
    sleep 2
  done
  SCALE_DOWN_END="$(date +%s)"
  SCALE_DOWN_DURATION=$((SCALE_DOWN_END - SCALE_DOWN_START))
fi

MANIFEST_JSON="${LOCAL_OUT}/pvc_snapshot_manifest.json"
SNAPSHOT_TIMING_JSON="${LOCAL_OUT}/pvc_snapshot_timing.json"
DISKS_JSON="${LOCAL_OUT}/pvc_snapshot_disks.jsonl"
: > "${DISKS_JSON}"

SNAPSHOT_CREATE_START="$(date +%s)"
for ordinal in 0 1 2 3 4 5; do
  pvc_name="valkey-data-${RELEASE}-${ordinal}"
  disk_name="$(pvc_disk_name "${pvc_name}")"
  storage_request="$(pvc_storage_request "${pvc_name}")"
  storage_class="$(pvc_storage_class "${pvc_name}")"
  access_modes="$(pvc_access_modes_json "${pvc_name}")"
  snapshot_name="${SNAPSHOT_PREFIX}-${ordinal}"

  echo "  Snapshot ${pvc_name} (${disk_name}) -> ${snapshot_name}"
  one_start="$(date +%s)"
  gcloud compute snapshots create "${snapshot_name}" \
    --project="${PROJECT_ID}" \
    --source-disk="${disk_name}" \
    --source-disk-zone="${ZONE}" \
    --storage-location="${LOCATION}" \
    --quiet
  one_end="$(date +%s)"

  "${PYTHON_BIN}" -c '
import json, sys
print(json.dumps({
  "ordinal": int(sys.argv[1]),
  "pvc_name": sys.argv[2],
  "source_disk": sys.argv[3],
  "snapshot": sys.argv[4],
  "storage_request": sys.argv[5],
  "storage_class": sys.argv[6],
  "access_modes": json.loads(sys.argv[7]),
  "snapshot_duration_s": int(sys.argv[8]) - int(sys.argv[9]),
}))
' "${ordinal}" "${pvc_name}" "${disk_name}" "${snapshot_name}" "${storage_request}" "${storage_class}" "${access_modes}" "${one_end}" "${one_start}" >> "${DISKS_JSON}"
done
SNAPSHOT_CREATE_END="$(date +%s)"

"${PYTHON_BIN}" -c '
import json, sys
items = [json.loads(line) for line in open(sys.argv[1]) if line.strip()]
doc = {
  "project_id": sys.argv[2],
  "location": sys.argv[3],
  "zone": sys.argv[4],
  "namespace": sys.argv[5],
  "release": sys.argv[6],
  "snapshot_prefix": sys.argv[7],
  "items": items,
}
json.dump(doc, open(sys.argv[8], "w"), indent=2)
' "${DISKS_JSON}" "${PROJECT_ID}" "${LOCATION}" "${ZONE}" "${NS}" "${RELEASE}" "${SNAPSHOT_PREFIX}" "${MANIFEST_JSON}"

BACKUP_END="$(date +%s)"
cat > "${SNAPSHOT_TIMING_JSON}" <<EOF
{
  "project_id": $(json_string "${PROJECT_ID}"),
  "location": $(json_string "${LOCATION}"),
  "zone": $(json_string "${ZONE}"),
  "namespace": $(json_string "${NS}"),
  "release": $(json_string "${RELEASE}"),
  "snapshot_prefix": $(json_string "${SNAPSHOT_PREFIX}"),
  "manifest": $(json_string "${MANIFEST_JSON}"),
  "backup_start": ${BACKUP_START},
  "bgsave_start": ${BGSAVE_START},
  "bgsave_end": ${BGSAVE_END},
  "bgsave_duration_s": ${BGSAVE_DURATION},
  "scale_down_start": ${SCALE_DOWN_START},
  "scale_down_end": ${SCALE_DOWN_END},
  "scale_down_duration_s": ${SCALE_DOWN_DURATION},
  "snapshot_create_start": ${SNAPSHOT_CREATE_START},
  "snapshot_create_end": ${SNAPSHOT_CREATE_END},
  "snapshot_create_duration_s": $((SNAPSHOT_CREATE_END - SNAPSHOT_CREATE_START)),
  "backup_end": ${BACKUP_END},
  "backup_duration_s": $((BACKUP_END - BACKUP_START))
}
EOF

echo ""
echo "Backup manifest: ${MANIFEST_JSON}"
echo "Timing: ${SNAPSHOT_TIMING_JSON}"

if [[ "${SCALE_DOWN_FOR_SNAPSHOT}" == "true" ]]; then
  echo ""
  echo "Cluster is intentionally scaled to 0 after backup."
  echo "Scale it back with:"
  echo "  kubectl scale sts/${RELEASE} -n ${NS} --replicas=6"
fi

#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 <pvc_snapshot_manifest.json> [output-dir]

Restores Valkey PVCs from GCE Persistent Disk snapshots by creating new disks,
static PV/PVC objects, and reinstalling the Helm release. Writes timing JSON.

Defaults:
  PROJECT_ID=<gcloud active project>
  ZONE=<zone from manifest>
  NS=<namespace from manifest>
  RELEASE=<release from manifest>
  CHART_PATH=../valkey-helm/valkey
  VALUES_FILE=k8s/manifests/values-backup-max7gb.yaml
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || -z "${1:-}" ]]; then
  usage
  exit 0
fi

MANIFEST_JSON="$1"
LOCAL_OUT="${2:-./results/valkey_pvc_snapshot_restore}"
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
CHART_PATH="${CHART_PATH:-../valkey-helm/valkey}"
VALUES_FILE="${VALUES_FILE:-k8s/manifests/values-backup-max7gb.yaml}"
RESTORE_DISK_PREFIX="${RESTORE_DISK_PREFIX:-valkey-restore-$(date +%Y%m%d-%H%M%S)}"

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

manifest_field() {
  local field="$1"
  local default="$2"
  "${PYTHON_BIN}" -c '
import json, sys
doc = json.load(open(sys.argv[1]))
print(doc.get(sys.argv[2], sys.argv[3]))
' "${MANIFEST_JSON}" "${field}" "${default}"
}

NS="${NS:-$(manifest_field namespace vk)}"
RELEASE="${RELEASE:-$(manifest_field release valkey)}"
ZONE="${ZONE:-$(manifest_field zone europe-central2-a)}"

wait_cluster_healthy() {
  local max_wait="${1:-600}"
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

wait_for_pvcs_bound() {
  local expected="$1"
  local timeout_s="${2:-300}"
  local deadline=$((SECONDS + timeout_s))
  while (( SECONDS < deadline )); do
    local bound
    bound="$(kubectl get pvc -n "${NS}" -l app.kubernetes.io/instance="${RELEASE}",app.kubernetes.io/name=valkey \
      --no-headers 2>/dev/null | awk '$2 == "Bound" { c++ } END { print c + 0 }')" || true
    if [[ "${bound}" == "${expected}" ]]; then
      return 0
    fi
    sleep 2
  done
  kubectl get pvc -n "${NS}" || true
  return 1
}

manifest_items_base64() {
  "${PYTHON_BIN}" -c '
import base64, json, sys
d = json.load(open(sys.argv[1]))
for item in d["items"]:
    print(base64.b64encode(json.dumps(item).encode()).decode())
' "${MANIFEST_JSON}"
}

item_field() {
  local encoded="$1"
  local field="$2"
  "${PYTHON_BIN}" -c '
import base64, json, sys
item = json.loads(base64.b64decode(sys.argv[1]))
value = item.get(sys.argv[2], "")
if isinstance(value, list):
    print(" ".join(value))
else:
    print(value)
' "${encoded}" "${field}"
}

echo "==> Valkey PVC snapshot restore"
echo "PROJECT_ID=${PROJECT_ID}"
echo "ZONE=${ZONE}"
echo "NS=${NS}"
echo "RELEASE=${RELEASE}"
echo "RESTORE_DISK_PREFIX=${RESTORE_DISK_PREFIX}"
echo "VALUES_FILE=${VALUES_FILE}"

RESTORE_START="$(date +%s)"

echo "  Removing existing Helm release and PVCs if present..."
helm uninstall "${RELEASE}" -n "${NS}" --ignore-not-found || true
kubectl delete pvc -n "${NS}" -l app.kubernetes.io/instance="${RELEASE}",app.kubernetes.io/name=valkey --ignore-not-found --wait=true || true

DISK_CREATE_START="$(date +%s)"
RESTORED_JSONL="${LOCAL_OUT}/restored_disks.jsonl"
: > "${RESTORED_JSONL}"

for encoded in $(manifest_items_base64); do
  ordinal="$(item_field "${encoded}" ordinal)"
  pvc_name="$(item_field "${encoded}" pvc_name)"
  snapshot="$(item_field "${encoded}" snapshot)"
  storage_request="$(item_field "${encoded}" storage_request)"
  storage_class="$(item_field "${encoded}" storage_class)"
  disk_name="${RESTORE_DISK_PREFIX}-${ordinal}"

  echo "  Creating disk ${disk_name} from snapshot ${snapshot}"
  one_start="$(date +%s)"
  gcloud compute disks create "${disk_name}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --source-snapshot="${snapshot}" \
    --type=pd-balanced \
    --quiet
  one_end="$(date +%s)"

  "${PYTHON_BIN}" -c '
import json, sys
print(json.dumps({
  "ordinal": int(sys.argv[1]),
  "pvc_name": sys.argv[2],
  "snapshot": sys.argv[3],
  "restore_disk": sys.argv[4],
  "storage_request": sys.argv[5],
  "storage_class": sys.argv[6],
  "disk_create_duration_s": int(sys.argv[7]) - int(sys.argv[8]),
}))
' "${ordinal}" "${pvc_name}" "${snapshot}" "${disk_name}" "${storage_request}" "${storage_class}" "${one_end}" "${one_start}" >> "${RESTORED_JSONL}"
done
DISK_CREATE_END="$(date +%s)"

PV_CREATE_START="$(date +%s)"
for encoded in $(manifest_items_base64); do
  ordinal="$(item_field "${encoded}" ordinal)"
  pvc_name="$(item_field "${encoded}" pvc_name)"
  storage_request="$(item_field "${encoded}" storage_request)"
  storage_class="$(item_field "${encoded}" storage_class)"
  disk_name="${RESTORE_DISK_PREFIX}-${ordinal}"
  pv_name="${RESTORE_DISK_PREFIX}-pv-${ordinal}"

  echo "  Creating static PV/PVC for ${pvc_name}"
  kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolume
metadata:
  name: ${pv_name}
  labels:
    app.kubernetes.io/instance: ${RELEASE}
    app.kubernetes.io/name: valkey
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
    app.kubernetes.io/name: valkey
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: ${storage_request}
  storageClassName: ${storage_class}
  volumeName: ${pv_name}
EOF
done
wait_for_pvcs_bound 6 300
PV_CREATE_END="$(date +%s)"

HELM_INSTALL_START="$(date +%s)"
echo "  Installing Helm release ${RELEASE}..."
helm install "${RELEASE}" "${CHART_PATH}" -n "${NS}" -f "${VALUES_FILE}" --wait=false
kubectl rollout status sts/"${RELEASE}" -n "${NS}" --timeout=900s
PODS_READY_TS="$(date +%s)"

wait_cluster_healthy 600
READY_TS="$(date +%s)"

RESTORE_END="${READY_TS}"
TIMING_JSON="${LOCAL_OUT}/pvc_snapshot_restore_timing.json"

cat > "${TIMING_JSON}" <<EOF
{
  "project_id": $(json_string "${PROJECT_ID}"),
  "zone": $(json_string "${ZONE}"),
  "namespace": $(json_string "${NS}"),
  "release": $(json_string "${RELEASE}"),
  "manifest": $(json_string "${MANIFEST_JSON}"),
  "restore_disk_prefix": $(json_string "${RESTORE_DISK_PREFIX}"),
  "restore_start": ${RESTORE_START},
  "disk_create_start": ${DISK_CREATE_START},
  "disk_create_end": ${DISK_CREATE_END},
  "disk_create_duration_s": $((DISK_CREATE_END - DISK_CREATE_START)),
  "pv_create_start": ${PV_CREATE_START},
  "pv_create_end": ${PV_CREATE_END},
  "pv_create_duration_s": $((PV_CREATE_END - PV_CREATE_START)),
  "helm_install_start": ${HELM_INSTALL_START},
  "pods_ready_ts": ${PODS_READY_TS},
  "pod_recreate_duration_s": $((PODS_READY_TS - HELM_INSTALL_START)),
  "ready_ts": ${READY_TS},
  "cluster_recovery_after_pods_s": $((READY_TS - PODS_READY_TS)),
  "restore_end": ${RESTORE_END},
  "restore_duration_s": $((RESTORE_END - RESTORE_START))
}
EOF

echo ""
echo "Restore timing: ${TIMING_JSON}"

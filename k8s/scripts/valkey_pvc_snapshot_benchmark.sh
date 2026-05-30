#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 [output-dir]

Runs a full Valkey PVC snapshot backup/restore benchmark:
  1. Ensure a fresh Valkey cluster exists.
  2. Seed DATASET_MB data.
  3. Create PVC snapshots with timing.
  4. Restore PVCs from snapshots with timing.
  5. Verify restored data.
  6. Repeat N times.

Defaults:
  DATASET_MB=10240
  N=1
  NS=vk
  RELEASE=valkey
  CHART_PATH=../valkey-helm/valkey
  VALUES_FILE=k8s/manifests/values-backup-max7gb.yaml
  BACKUP_IMAGE=<LOCATION>-docker.pkg.dev/<PROJECT_ID>/valkey-bench/backup_restore:1
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

LOCAL_OUT="${1:-./results/valkey_pvc_snapshot_benchmark}"
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
LOCATION="${LOCATION:-europe-central2}"
ZONE="${ZONE:-europe-central2-a}"
NS="${NS:-vk}"
RELEASE="${RELEASE:-valkey}"
CHART_PATH="${CHART_PATH:-../valkey-helm/valkey}"
VALUES_FILE="${VALUES_FILE:-k8s/manifests/values-backup-max7gb.yaml}"
ARTIFACT_REPO="${ARTIFACT_REPO:-valkey-bench}"
BACKUP_IMAGE="${BACKUP_IMAGE:-${LOCATION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REPO}/backup_restore:1}"
DATASET_MB="${DATASET_MB:-10240}"
N="${N:-1}"
REMOTE_OUT="/work/results/backup"
RANDOM_DATA="${RANDOM_DATA:-false}"
RANDOM_DATA_ARG=""
if [[ "${RANDOM_DATA}" == "true" ]]; then
  RANDOM_DATA_ARG="--random-data"
fi
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "${SCRIPT_DIR}/pod_results.sh"

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
  local timeout_s="${2:-120}"

  kubectl wait pod/"${pod_name}" -n "${NS}" \
    --for=condition=Ready --timeout="${timeout_s}s"
}

wait_cluster_healthy() {
  local max_wait="${1:-600}"
  local elapsed=0
  echo "  Waiting for cluster to become healthy (max ${max_wait}s)..."
  while [[ ${elapsed} -lt ${max_wait} ]]; do
    local state
    state="$(kubectl exec "${RELEASE}-0" -c valkey -n "${NS}" -- \
      valkey-cli cluster info 2>/dev/null | grep cluster_state | tr -d '[:space:]')" || true
    if [[ "${state}" == "cluster_state:ok" ]]; then
      local slots
      slots="$(kubectl exec "${RELEASE}-0" -c valkey -n "${NS}" -- \
        valkey-cli cluster info 2>/dev/null | grep cluster_slots_ok | cut -d: -f2 | tr -d '[:space:]\r')" || true
      if [[ "${slots}" == "16384" ]]; then
        echo "  Cluster healthy after ${elapsed}s"
        return 0
      fi
    fi
    sleep 5
    elapsed=$((elapsed + 5))
  done

  echo "ERROR: Cluster not healthy after ${max_wait}s" >&2
  kubectl get pods -n "${NS}" || true
  kubectl exec "${RELEASE}-0" -c valkey -n "${NS}" -- valkey-cli cluster info || true
  kubectl exec "${RELEASE}-0" -c valkey -n "${NS}" -- valkey-cli cluster nodes || true
  return 1
}

retained_valkey_pvs_jsonl() {
  kubectl get pv -o json | "${PYTHON_BIN}" -c '
import json, sys
ns, release = sys.argv[1], sys.argv[2]
doc = json.load(sys.stdin)
prefix = f"valkey-data-{release}-"
for item in doc.get("items", []):
    spec = item.get("spec", {})
    claim = spec.get("claimRef") or {}
    if claim.get("namespace") != ns:
        continue
    if not str(claim.get("name", "")).startswith(prefix):
        continue
    if spec.get("persistentVolumeReclaimPolicy") != "Retain":
        continue
    handle = ((spec.get("csi") or {}).get("volumeHandle") or "")
    print(json.dumps({
        "pv": item["metadata"]["name"],
        "disk": handle.rsplit("/", 1)[-1] if handle else "",
    }))
' "${NS}" "${RELEASE}"
}

cleanup_retained_restore_storage() {
  local retained_jsonl
  retained_jsonl="$(mktemp)"
  retained_valkey_pvs_jsonl > "${retained_jsonl}" || true

  if [[ ! -s "${retained_jsonl}" ]]; then
    rm -f "${retained_jsonl}"
    return 0
  fi

  echo "  Cleaning retained restore PVs and disks from previous run..."
  "${PYTHON_BIN}" -c '
import json, sys
for line in open(sys.argv[1]):
    if line.strip():
        print(json.loads(line)["pv"])
' "${retained_jsonl}" | xargs -r kubectl delete pv --ignore-not-found

  while read -r disk_name; do
    [[ -z "${disk_name}" ]] && continue
    gcloud compute disks delete "${disk_name}" \
      --project="${PROJECT_ID}" \
      --zone="${ZONE}" \
      --quiet || true
  done < <("${PYTHON_BIN}" -c '
import json, sys
for line in open(sys.argv[1]):
    if line.strip():
        print(json.loads(line).get("disk", ""))
' "${retained_jsonl}")

  rm -f "${retained_jsonl}"
}

delete_release_and_pvcs() {
  helm uninstall "${RELEASE}" -n "${NS}" --ignore-not-found || true
  while kubectl get pod -n "${NS}" -l app.kubernetes.io/instance="${RELEASE}",app.kubernetes.io/name=valkey --no-headers 2>/dev/null | grep -q .; do
    sleep 2
  done
  kubectl delete pvc -n "${NS}" -l app.kubernetes.io/instance="${RELEASE}",app.kubernetes.io/name=valkey --ignore-not-found --wait=true || true
  cleanup_retained_restore_storage
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
  local pod_name="backup-seed-${run_idx}"

  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl run "${pod_name}" -n "${NS}" \
    --image="${BACKUP_IMAGE}" \
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
  local pod_name="backup-verify-${run_idx}"

  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl run "${pod_name}" -n "${NS}" \
    --image="${BACKUP_IMAGE}" \
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

write_run_summary() {
  local run_idx="$1"
  local run_dir="$2"
  local run_id="$3"
  local seed_report="$4"
  local verify_report="$5"
  local backup_timing="${run_dir}/pvc_snapshot_timing.json"
  local restore_timing="${run_dir}/restore/pvc_snapshot_restore_timing.json"
  local summary_file="${run_dir}/pvc_snapshot_benchmark_timing.json"

  "${PYTHON_BIN}" -c '
import json, sys
run = int(sys.argv[1])
run_id = sys.argv[2]
seed = json.load(open(sys.argv[3]))
verify = json.load(open(sys.argv[4]))
backup = json.load(open(sys.argv[5]))
restore = json.load(open(sys.argv[6]))
doc = {
    "run": run,
    "run_id": run_id,
    "dataset_mb": seed.get("target_mb"),
    "seed_keys": seed.get("written_keys"),
    "seed_duration_s": seed.get("seed_duration_s"),
    "backup_duration_s": backup.get("backup_duration_s"),
    "bgsave_duration_s": backup.get("bgsave_duration_s"),
    "scale_down_duration_s": backup.get("scale_down_duration_s"),
    "snapshot_create_duration_s": backup.get("snapshot_create_duration_s"),
    "restore_duration_s": restore.get("restore_duration_s"),
    "disk_create_duration_s": restore.get("disk_create_duration_s"),
    "pv_create_duration_s": restore.get("pv_create_duration_s"),
    "pod_recreate_duration_s": restore.get("pod_recreate_duration_s"),
    "cluster_recovery_after_pods_s": restore.get("cluster_recovery_after_pods_s"),
    "verify_duration_s": verify.get("verify_duration_s"),
    "verify_sample_size": verify.get("sample_size"),
    "keys_missing": verify.get("keys_missing"),
    "verify_errors": verify.get("verify_errors"),
    "integrity_ok": verify.get("integrity_ok"),
}
json.dump(doc, open(sys.argv[7], "w"), indent=2)
' "${run_idx}" "${run_id}" "${run_dir}/${seed_report}" "${run_dir}/${verify_report}" "${backup_timing}" "${restore_timing}" "${summary_file}"
}

echo "==> Valkey PVC snapshot backup/restore benchmark"
echo "PROJECT_ID=${PROJECT_ID}"
echo "LOCATION=${LOCATION}"
echo "ZONE=${ZONE}"
echo "NS=${NS}"
echo "RELEASE=${RELEASE}"
echo "DATASET_MB=${DATASET_MB}"
echo "N=${N}"
echo "VALUES_FILE=${VALUES_FILE}"

for i in $(seq 1 "${N}"); do
  echo ""
  echo "=========================================="
  echo "  Valkey PVC snapshot run ${i}/${N}"
  echo "=========================================="

  RUN_DIR="${LOCAL_OUT}/run_${i}"
  RESTORE_DIR="${RUN_DIR}/restore"
  mkdir -p "${RUN_DIR}" "${RESTORE_DIR}"

  RUN_ID="valkey-pvc-snapshot-${DATASET_MB}mb-${i}-$(date +%s)"
  SEED_REPORT="seed_report_valkey_${DATASET_MB}_${i}.json"
  VERIFY_REPORT="verify_report_valkey_${DATASET_MB}_${i}.json"

  install_fresh_cluster

  echo "[${i}] Seeding ${DATASET_MB} MB..."
  seed_data "${i}" "${RUN_DIR}" "${RUN_ID}" "${SEED_REPORT}"

  echo "[${i}] Creating PVC snapshot backup..."
  SNAPSHOT_PREFIX="${RELEASE}-pvc-bench-${i}-$(date +%Y%m%d-%H%M%S)" \
  PROJECT_ID="${PROJECT_ID}" LOCATION="${LOCATION}" ZONE="${ZONE}" NS="${NS}" RELEASE="${RELEASE}" \
    "${SCRIPT_DIR}/valkey_pvc_snapshot_backup.sh" "${RUN_DIR}"

  echo "[${i}] Restoring from PVC snapshots..."
  RESTORE_DISK_PREFIX="${RELEASE}-restore-bench-${i}-$(date +%Y%m%d-%H%M%S)" \
  PROJECT_ID="${PROJECT_ID}" ZONE="${ZONE}" NS="${NS}" RELEASE="${RELEASE}" \
  CHART_PATH="${CHART_PATH}" VALUES_FILE="${VALUES_FILE}" \
    "${SCRIPT_DIR}/valkey_pvc_snapshot_restore.sh" "${RUN_DIR}/pvc_snapshot_manifest.json" "${RESTORE_DIR}"

  echo "[${i}] Verifying restored data..."
  verify_data "${i}" "${RUN_DIR}" "${SEED_REPORT}" "${VERIFY_REPORT}"

  write_run_summary "${i}" "${RUN_DIR}" "${RUN_ID}" "${SEED_REPORT}" "${VERIFY_REPORT}"
  echo "[${i}] Summary: ${RUN_DIR}/pvc_snapshot_benchmark_timing.json"
done

echo ""
echo "=========================================="
echo "  All ${N} Valkey PVC snapshot benchmark runs complete."
echo "  Results in: ${LOCAL_OUT}/"
echo "=========================================="

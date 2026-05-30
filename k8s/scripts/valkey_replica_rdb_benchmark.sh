#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 <dataset_mb_total> [output-dir]

Runs the full Valkey online replica-RDB backup/restore benchmark:
  1. flush source cluster
  2. seed dataset
  3. create online RDB backup from replicas
  4. restore RDBs into a fresh Helm release/PVCs
  5. verify restored data

Environment:
  N=1
  NS=vk
  RELEASE=valkey
  HOST=valkey.vk.svc.cluster.local
  PORT=6379
  RANDOM_DATA=true
  BACKUP_IMAGE=<artifact-registry>/backup_restore:1
  IMAGE_PULL_POLICY=Always
  VALUES_FILE=k8s/manifests/values.yaml
  CHART_PATH=../valkey-helm/valkey
  COPY_IMAGE=busybox:1.36
  EXPECTED_SHARDS=3
  FLUSH_BEFORE_SEED=true
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

DATASET_MB="${1:?Usage: $0 <dataset_mb_total> [output-dir]}"
LOCAL_OUT="${2:-./results/valkey_replica_rdb_benchmark}"
N="${N:-1}"
NS="${NS:-vk}"
RELEASE="${RELEASE:-valkey}"
HOST="${HOST:-${RELEASE}.${NS}.svc.cluster.local}"
PORT="${PORT:-6379}"
LOCATION="${LOCATION:-europe-central2}"
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
ARTIFACT_REPO="${ARTIFACT_REPO:-valkey-bench}"
if [[ -n "${BACKUP_IMAGE:-}" ]]; then
  BACKUP_IMAGE="${BACKUP_IMAGE}"
elif [[ -n "${PROJECT_ID}" && "${PROJECT_ID}" != "(unset)" ]]; then
  BACKUP_IMAGE="${LOCATION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REPO}/backup_restore:1"
else
  BACKUP_IMAGE="backup_restore:1"
fi
IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-Always}"
VALUES_FILE="${VALUES_FILE:-k8s/manifests/values.yaml}"
CHART_PATH="${CHART_PATH:-../valkey-helm/valkey}"
COPY_IMAGE="${COPY_IMAGE:-busybox:1.36}"
EXPECTED_SHARDS="${EXPECTED_SHARDS:-3}"
RANDOM_DATA="${RANDOM_DATA:-true}"
FLUSH_BEFORE_SEED="${FLUSH_BEFORE_SEED:-true}"
SEED_TIMEOUT_SECONDS="${SEED_TIMEOUT_SECONDS:-7200}"
VERIFY_TIMEOUT_SECONDS="${VERIFY_TIMEOUT_SECONDS:-3600}"
REMOTE_OUT="/work/results/replica_rdb_benchmark"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON_BIN="$(command -v python3 || command -v python || true)"

if [[ -z "${PYTHON_BIN}" ]]; then
  echo "ERROR: python3 or python is required." >&2
  exit 1
fi

source "${SCRIPT_DIR}/pod_results.sh"

mkdir -p "${LOCAL_OUT}"

RANDOM_DATA_ARG=""
if [[ "${RANDOM_DATA}" == "true" ]]; then
  RANDOM_DATA_ARG="--random-data"
fi

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
      if [[ "${masters}" -ge "${EXPECTED_SHARDS}" ]]; then
        echo "  Cluster healthy (${masters} masters) after ${elapsed}s"
        return 0
      fi
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

run_flush() {
  local run_no="$1"
  local pod_name="replica-rdb-flush-${run_no}"

  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl run "${pod_name}" -n "${NS}" \
    --image="${BACKUP_IMAGE}" \
    --image-pull-policy="${IMAGE_PULL_POLICY}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      python /work/backup_restore_seed.py \
        --mode flush \
        --host '${HOST}' --port '${PORT}' \
        --reset-stats
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  wait_for_command_pod "${pod_name}" 900
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

run_seed() {
  local run_no="$1"
  local run_id="$2"
  local local_report="$3"
  local remote_report="${REMOTE_OUT}/$(basename "${local_report}")"
  local pod_name="replica-rdb-seed-${run_no}"

  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl run "${pod_name}" -n "${NS}" \
    --image="${BACKUP_IMAGE}" \
    --image-pull-policy="${IMAGE_PULL_POLICY}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_OUT}'
      python /work/backup_restore_seed.py \
        --mode seed \
        --host '${HOST}' --port '${PORT}' \
        --target-mb '${DATASET_MB}' \
        --run-id '${run_id}' \
        ${RANDOM_DATA_ARG} \
        --output '${remote_report}'
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  wait_for_command_pod "${pod_name}" "${SEED_TIMEOUT_SECONDS}"
  kubectl cp "${NS}/${pod_name}:${remote_report}" "${local_report}"
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

run_verify() {
  local run_no="$1"
  local seed_report="$2"
  local local_report="$3"
  local seed_name
  local verify_name
  local pod_name="replica-rdb-verify-${run_no}"
  seed_name="$(basename "${seed_report}")"
  verify_name="$(basename "${local_report}")"

  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl run "${pod_name}" -n "${NS}" \
    --image="${BACKUP_IMAGE}" \
    --image-pull-policy="${IMAGE_PULL_POLICY}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_OUT}'
      while [ ! -f '${REMOTE_OUT}/${seed_name}' ]; do sleep 1; done
      python /work/backup_restore_seed.py \
        --mode verify \
        --host '${HOST}' --port '${PORT}' \
        --seed-report '${REMOTE_OUT}/${seed_name}' \
        --output '${REMOTE_OUT}/${verify_name}'
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  wait_for_pod_ready "${pod_name}" 120
  kubectl cp "${seed_report}" "${NS}/${pod_name}:${REMOTE_OUT}/${seed_name}"
  wait_for_command_pod "${pod_name}" "${VERIFY_TIMEOUT_SECONDS}"
  kubectl cp "${NS}/${pod_name}:${REMOTE_OUT}/${verify_name}" "${local_report}"
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

latest_manifest() {
  local backup_out="$1"
  find "${backup_out}" -name replica_rdb_manifest.json -print | sort | tail -1
}

write_run_summary() {
  local run_no="$1"
  local run_id="$2"
  local run_dir="$3"
  local seed_report="$4"
  local backup_manifest="$5"
  local restore_timing="$6"
  local verify_report="$7"
  local run_start="$8"
  local run_end="$9"
  local output="${run_dir}/replica_rdb_benchmark_run_${run_no}.json"

  "${PYTHON_BIN}" - "${run_no}" "${run_id}" "${DATASET_MB}" "${run_start}" "${run_end}" \
    "${seed_report}" "${backup_manifest}" "${restore_timing}" "${verify_report}" "${output}" <<'PY'
import json
import sys

(
    run_no, run_id, dataset_mb, run_start, run_end,
    seed_path, manifest_path, restore_path, verify_path, output_path,
) = sys.argv[1:]

seed = json.load(open(seed_path))
manifest = json.load(open(manifest_path))
restore = json.load(open(restore_path))
verify = json.load(open(verify_path))
rdb_size = sum(int(shard.get("rdb_size_bytes", 0)) for shard in manifest.get("shards", []))

summary = {
    "variant": "valkey_replica_rdb_backup_restore",
    "run": int(run_no),
    "run_id": run_id,
    "dataset_mb_total": int(dataset_mb),
    "random_data": bool(seed.get("random_data", False)),
    "run_start": int(run_start),
    "run_end": int(run_end),
    "run_duration_s": int(run_end) - int(run_start),
    "seed_duration_s": seed.get("seed_duration_s"),
    "seed_completed": seed.get("completed"),
    "written_keys": seed.get("written_keys"),
    "written_bytes": seed.get("total_bytes"),
    "backup_duration_s": manifest.get("backup_duration_s"),
    "replica_bgsave_duration_s": manifest.get("save_duration_s"),
    "rdb_validation_duration_s": manifest.get("rdb_validation_duration_s"),
    "backup_copy_duration_s": manifest.get("backup_copy_duration_s"),
    "rdb_total_size_bytes": rdb_size,
    "rdb_total_size_mb": round(rdb_size / 1024 / 1024, 2),
    "fresh_cluster_create_duration_s": restore.get("fresh_cluster_ready_ts", 0) - restore.get("restore_start", 0),
    "scale_down_duration_s": restore.get("scale_down_duration_s"),
    "rdb_injection_duration_s": restore.get("rdb_injection_duration_s"),
    "pod_recreate_duration_s": restore.get("pod_recreate_duration_s"),
    "cluster_recovery_after_pods_s": restore.get("cluster_recovery_after_pods_s"),
    "restore_duration_s": restore.get("restore_duration_s"),
    "verify_duration_s": verify.get("verify_duration_s"),
    "integrity_ok": verify.get("integrity_ok"),
    "seed_report": seed_path,
    "backup_manifest": manifest_path,
    "restore_timing": restore_path,
    "verify_report": verify_path,
}

with open(output_path, "w") as fh:
    json.dump(summary, fh, indent=2)
    fh.write("\n")

print(output_path)
PY
}

write_aggregate_csv() {
  local output="${LOCAL_OUT}/replica_rdb_benchmark_summary.csv"

  "${PYTHON_BIN}" - "${LOCAL_OUT}" "${output}" <<'PY'
import csv
import glob
import json
import os
import sys

root, output = sys.argv[1:]
paths = sorted(glob.glob(os.path.join(root, "run_*", "replica_rdb_benchmark_run_*.json")))
rows = [json.load(open(path)) for path in paths]
fields = [
    "run",
    "dataset_mb_total",
    "random_data",
    "run_duration_s",
    "seed_duration_s",
    "backup_duration_s",
    "replica_bgsave_duration_s",
    "rdb_validation_duration_s",
    "backup_copy_duration_s",
    "rdb_total_size_mb",
    "fresh_cluster_create_duration_s",
    "rdb_injection_duration_s",
    "pod_recreate_duration_s",
    "cluster_recovery_after_pods_s",
    "restore_duration_s",
    "verify_duration_s",
    "integrity_ok",
]

with open(output, "w", newline="") as fh:
    writer = csv.DictWriter(fh, fieldnames=fields)
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field) for field in fields})

print(output)
PY
}

echo "==> Valkey replica-RDB backup/restore benchmark"
echo "NS=${NS}"
echo "RELEASE=${RELEASE}"
echo "HOST=${HOST}"
echo "PORT=${PORT}"
echo "DATASET_MB=${DATASET_MB}"
echo "N=${N}"
echo "RANDOM_DATA=${RANDOM_DATA}"
echo "BACKUP_IMAGE=${BACKUP_IMAGE}"
echo "VALUES_FILE=${VALUES_FILE}"
echo "OUTPUT=${LOCAL_OUT}"

for i in $(seq 1 "${N}"); do
  run_dir="${LOCAL_OUT}/run_${i}"
  backup_out="${run_dir}/backup"
  restore_out="${run_dir}/restore"
  seed_report="${run_dir}/seed_report_${DATASET_MB}_${i}.json"
  verify_report="${run_dir}/verify_report_${DATASET_MB}_${i}.json"
  run_id="replica_rdb_${DATASET_MB}mb_${i}_$(date +%s)"

  mkdir -p "${run_dir}" "${backup_out}" "${restore_out}"

  echo ""
  echo "=========================================="
  echo "  Valkey replica-RDB run ${i}/${N} (${DATASET_MB} MB total)"
  echo "=========================================="

  run_start="$(date +%s)"
  wait_cluster_healthy 600

  if [[ "${FLUSH_BEFORE_SEED}" == "true" ]]; then
    echo "[${i}] Flushing source cluster..."
    run_flush "${i}"
  fi

  echo "[${i}] Seeding dataset..."
  run_seed "${i}" "${run_id}" "${seed_report}"

  echo "[${i}] Creating online replica RDB backup..."
  NS="${NS}" \
  RELEASE="${RELEASE}" \
  VALUES_FILE="${VALUES_FILE}" \
  CHART_PATH="${CHART_PATH}" \
  COPY_IMAGE="${COPY_IMAGE}" \
  EXPECTED_SHARDS="${EXPECTED_SHARDS}" \
    "${SCRIPT_DIR}/valkey_replica_rdb_backup_restore.sh" backup "${backup_out}"

  manifest="$(latest_manifest "${backup_out}")"
  if [[ -z "${manifest}" || ! -f "${manifest}" ]]; then
    echo "ERROR: backup manifest not found under ${backup_out}" >&2
    exit 1
  fi

  echo "[${i}] Restoring backup into fresh cluster..."
  NS="${NS}" \
  RELEASE="${RELEASE}" \
  VALUES_FILE="${VALUES_FILE}" \
  CHART_PATH="${CHART_PATH}" \
  COPY_IMAGE="${COPY_IMAGE}" \
  EXPECTED_SHARDS="${EXPECTED_SHARDS}" \
    "${SCRIPT_DIR}/valkey_replica_rdb_backup_restore.sh" restore "${manifest}" "${restore_out}"

  echo "[${i}] Verifying restored dataset..."
  run_verify "${i}" "${seed_report}" "${verify_report}"

  run_end="$(date +%s)"
  summary_path="$(write_run_summary "${i}" "${run_id}" "${run_dir}" \
    "${seed_report}" "${manifest}" "${restore_out}/replica_rdb_restore_timing.json" \
    "${verify_report}" "${run_start}" "${run_end}")"
  echo "[${i}] Run summary: ${summary_path}"
done

csv_path="$(write_aggregate_csv)"

echo ""
echo "=========================================="
echo "  All ${N} replica-RDB benchmark runs complete."
echo "  Results in: ${LOCAL_OUT}"
echo "  CSV summary: ${csv_path}"
echo "=========================================="

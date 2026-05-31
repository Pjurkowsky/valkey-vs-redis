#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0

Creates or reuses a GKE benchmark cluster, installs monitoring, and installs
either Valkey Cluster or Redis 7.2 Cluster.

Environment:
  TARGET=valkey|redis72

  PROJECT_ID=<gcp-project>                 default: gcloud active project
  LOCATION=europe-central2
  NODE_LOCATIONS=europe-central2-a
  CLUSTER_NAME=valkey-bench
  NUM_NODES=3
  MACHINE_TYPE=n2-standard-4
  DISK_TYPE=pd-balanced
  DISK_SIZE_GB=50
  GKE_EXTRA_ARGS=""                        optional extra args for cluster create

  NS=<namespace>                           default: valkey for TARGET=valkey, redis for TARGET=redis72
  RELEASE=<helm-release>                   default: valkey or redis72
  VALUES_FILE=<values-file>                default: k8s/manifests/values.yaml or values-redis72.yaml

  MONITORING_NAMESPACE=monitoring
  MONITORING_RELEASE=monitoring
  INSTALL_MONITORING=true
  INSTALL_STATSD_EXPORTER=true

  VALKEY_CHART_PATH=../valkey-helm/valkey
  AUTO_PREPARE_VALKEY_CHART=true           clone/fetch PR 116 if chart is missing
  REDIS_CHART_PATH=oci://registry-1.docker.io/bitnamicharts/redis-cluster

  DELETE_UNATTACHED_DISKS=false            destructive when true
  DISK_CLEANUP_ZONES=<zones>               default: NODE_LOCATIONS
  DISK_CLEANUP_NAME_REGEX=""               optional regex filter for disk names
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if ! command -v gcloud >/dev/null 2>&1 && [[ -x "${HOME}/google-cloud-sdk/bin/gcloud" ]]; then
  export PATH="${HOME}/google-cloud-sdk/bin:${PATH}"
fi

require_cmd() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "ERROR: required command not found: ${cmd}" >&2
    exit 1
  fi
}

require_cmd gcloud
require_cmd kubectl
require_cmd helm
require_cmd python3

TARGET="${TARGET:-valkey}"
LOCATION="${LOCATION:-europe-central2}"
NODE_LOCATIONS="${NODE_LOCATIONS:-europe-central2-a}"
CLUSTER_NAME="${CLUSTER_NAME:-valkey-bench}"
NUM_NODES="${NUM_NODES:-3}"
MACHINE_TYPE="${MACHINE_TYPE:-n2-standard-4}"
DISK_TYPE="${DISK_TYPE:-pd-balanced}"
DISK_SIZE_GB="${DISK_SIZE_GB:-50}"
GKE_EXTRA_ARGS="${GKE_EXTRA_ARGS:-}"
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"

MONITORING_NAMESPACE="${MONITORING_NAMESPACE:-monitoring}"
MONITORING_RELEASE="${MONITORING_RELEASE:-monitoring}"
INSTALL_MONITORING="${INSTALL_MONITORING:-true}"
INSTALL_STATSD_EXPORTER="${INSTALL_STATSD_EXPORTER:-true}"

VALKEY_CHART_PATH="${VALKEY_CHART_PATH:-../valkey-helm/valkey}"
AUTO_PREPARE_VALKEY_CHART="${AUTO_PREPARE_VALKEY_CHART:-true}"
REDIS_CHART_PATH="${REDIS_CHART_PATH:-oci://registry-1.docker.io/bitnamicharts/redis-cluster}"

DELETE_UNATTACHED_DISKS="${DELETE_UNATTACHED_DISKS:-false}"
DISK_CLEANUP_ZONES="${DISK_CLEANUP_ZONES:-${NODE_LOCATIONS}}"
DISK_CLEANUP_NAME_REGEX="${DISK_CLEANUP_NAME_REGEX:-}"

case "${TARGET}" in
  valkey)
    NS="${NS:-valkey}"
    RELEASE="${RELEASE:-valkey}"
    VALUES_FILE="${VALUES_FILE:-k8s/manifests/values.yaml}"
    CHART_PATH="${CHART_PATH:-${VALKEY_CHART_PATH}}"
    STS_NAME="${STS_NAME:-${RELEASE}}"
    ;;
  redis|redis72)
    TARGET="redis72"
    NS="${NS:-redis}"
    RELEASE="${RELEASE:-redis72}"
    VALUES_FILE="${VALUES_FILE:-k8s/manifests/values-redis72.yaml}"
    CHART_PATH="${CHART_PATH:-${REDIS_CHART_PATH}}"
    STS_NAME="${STS_NAME:-${RELEASE}-redis-cluster}"
    ;;
  *)
    echo "ERROR: TARGET must be valkey or redis72." >&2
    exit 1
    ;;
esac

if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "(unset)" ]]; then
  echo "ERROR: PROJECT_ID is not set and gcloud has no active project." >&2
  exit 1
fi

print_config() {
  cat <<EOF
==> Benchmark environment setup
TARGET=${TARGET}
PROJECT_ID=${PROJECT_ID}
LOCATION=${LOCATION}
NODE_LOCATIONS=${NODE_LOCATIONS}
CLUSTER_NAME=${CLUSTER_NAME}
NUM_NODES=${NUM_NODES}
MACHINE_TYPE=${MACHINE_TYPE}
DISK_TYPE=${DISK_TYPE}
DISK_SIZE_GB=${DISK_SIZE_GB}
NS=${NS}
RELEASE=${RELEASE}
VALUES_FILE=${VALUES_FILE}
CHART_PATH=${CHART_PATH}
STS_NAME=${STS_NAME}
INSTALL_MONITORING=${INSTALL_MONITORING}
MONITORING_NAMESPACE=${MONITORING_NAMESPACE}
MONITORING_RELEASE=${MONITORING_RELEASE}
INSTALL_STATSD_EXPORTER=${INSTALL_STATSD_EXPORTER}
DELETE_UNATTACHED_DISKS=${DELETE_UNATTACHED_DISKS}
DISK_CLEANUP_ZONES=${DISK_CLEANUP_ZONES}
DISK_CLEANUP_NAME_REGEX=${DISK_CLEANUP_NAME_REGEX}
EOF
}

cluster_exists() {
  gcloud container clusters describe "${CLUSTER_NAME}" \
    --project="${PROJECT_ID}" \
    --location="${LOCATION}" >/dev/null 2>&1
}

create_or_reuse_gke_cluster() {
  if cluster_exists; then
    echo "  GKE cluster ${CLUSTER_NAME} already exists; reusing it."
  else
    echo "  Creating GKE cluster ${CLUSTER_NAME}..."
    local extra_args=()
    if [[ -n "${GKE_EXTRA_ARGS}" ]]; then
      # shellcheck disable=SC2206
      extra_args=(${GKE_EXTRA_ARGS})
    fi

    gcloud container clusters create "${CLUSTER_NAME}" \
      --project="${PROJECT_ID}" \
      --location="${LOCATION}" \
      --node-locations="${NODE_LOCATIONS}" \
      --num-nodes="${NUM_NODES}" \
      --machine-type="${MACHINE_TYPE}" \
      --disk-type="${DISK_TYPE}" \
      --disk-size="${DISK_SIZE_GB}" \
      "${extra_args[@]}"
  fi

  echo "  Fetching cluster credentials..."
  gcloud container clusters get-credentials "${CLUSTER_NAME}" \
    --project="${PROJECT_ID}" \
    --location="${LOCATION}"
}

install_monitoring() {
  if [[ "${INSTALL_MONITORING}" != "true" ]]; then
    echo "  Skipping monitoring install."
    return 0
  fi

  echo "  Installing Prometheus/Grafana (${MONITORING_RELEASE})..."
  helm repo add prometheus-community https://prometheus-community.github.io/helm-charts --force-update >/dev/null
  helm repo update >/dev/null
  helm upgrade --install "${MONITORING_RELEASE}" prometheus-community/kube-prometheus-stack \
    -n "${MONITORING_NAMESPACE}" \
    --create-namespace \
    --wait \
    --timeout=15m

  if [[ "${INSTALL_STATSD_EXPORTER}" == "true" ]]; then
    echo "  Installing StatsD exporter..."
    python3 - "${MONITORING_NAMESPACE}" "${MONITORING_RELEASE}" <<'PY' | kubectl apply -f -
import pathlib
import sys

namespace, release = sys.argv[1:3]
base = pathlib.Path("k8s/manifests")
statsd = (base / "statsd-exporter.yaml").read_text()
monitor = (base / "statsd-exporter-servicemonitor.yaml").read_text()

statsd = statsd.replace("name: monitoring", f"name: {namespace}", 1)
statsd = statsd.replace("namespace: monitoring", f"namespace: {namespace}")
monitor = monitor.replace("namespace: monitoring", f"namespace: {namespace}")
monitor = monitor.replace("release: monitoring", f"release: {release}")
monitor = monitor.replace("- monitoring", f"- {namespace}")

print(statsd)
print("---")
print(monitor)
PY
  fi
}

ensure_storage_classes() {
  if kubectl get storageclass pd-ssd-rwo >/dev/null 2>&1; then
    return 0
  fi

  echo "  Creating pd-ssd-rwo StorageClass..."
  kubectl apply -f - <<'EOF'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: pd-ssd-rwo
provisioner: pd.csi.storage.gke.io
parameters:
  type: pd-ssd
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: WaitForFirstConsumer
EOF
}

prepare_valkey_chart_if_needed() {
  if [[ "${TARGET}" != "valkey" || -d "${CHART_PATH}" || "${AUTO_PREPARE_VALKEY_CHART}" != "true" ]]; then
    return 0
  fi

  local chart_root
  chart_root="$(dirname "${CHART_PATH}")"
  local repo_root
  repo_root="$(dirname "${chart_root}")"

  echo "  Valkey chart not found at ${CHART_PATH}; preparing ${repo_root}/valkey-helm..."
  mkdir -p "${repo_root}"
  if [[ ! -d "${repo_root}/valkey-helm/.git" ]]; then
    git clone https://github.com/valkey-io/valkey-helm.git "${repo_root}/valkey-helm"
  fi

  git -C "${repo_root}/valkey-helm" fetch origin pull/116/head:pr-116
  git -C "${repo_root}/valkey-helm" checkout pr-116
}

install_target() {
  prepare_valkey_chart_if_needed

  echo "  Installing ${TARGET} release ${RELEASE} in namespace ${NS}..."
  kubectl create namespace "${NS}" --dry-run=client -o yaml | kubectl apply -f -

  helm upgrade --install "${RELEASE}" "${CHART_PATH}" \
    -n "${NS}" \
    -f "${VALUES_FILE}" \
    --create-namespace \
    --wait=false

  echo "  Waiting for StatefulSet ${STS_NAME}..."
  kubectl rollout status sts/"${STS_NAME}" -n "${NS}" --timeout=900s
}

cleanup_unattached_disks() {
  if [[ "${DELETE_UNATTACHED_DISKS}" != "true" ]]; then
    echo "  Skipping unattached disk cleanup. Set DELETE_UNATTACHED_DISKS=true to enable."
    return 0
  fi

  echo "  Looking for unattached disks in zones: ${DISK_CLEANUP_ZONES}"
  local zones_filter
  zones_filter="$(echo "${DISK_CLEANUP_ZONES}" | tr ',' ' ')"

  local disks_file
  disks_file="$(mktemp)"
  gcloud compute disks list \
    --project="${PROJECT_ID}" \
    --filter="zone:(${zones_filter})" \
    --format=json > "${disks_file}"

  local candidates_file
  candidates_file="$(mktemp)"
  python3 - "${disks_file}" "${DISK_CLEANUP_NAME_REGEX}" > "${candidates_file}" <<'PY'
import json
import re
import sys

path, name_regex = sys.argv[1:3]
pattern = re.compile(name_regex) if name_regex else None
for disk in json.load(open(path)):
    name = disk.get("name", "")
    users = disk.get("users") or []
    if users:
        continue
    if pattern and not pattern.search(name):
        continue
    zone = disk.get("zone", "").rsplit("/", 1)[-1]
    size = disk.get("sizeGb", "")
    dtype = disk.get("type", "").rsplit("/", 1)[-1]
    print(f"{name}\t{zone}\t{size}\t{dtype}")
PY

  if [[ ! -s "${candidates_file}" ]]; then
    echo "  No unattached disks matched cleanup filters."
    rm -f "${disks_file}" "${candidates_file}"
    return 0
  fi

  echo "  Deleting unattached disks:"
  column -t -s $'\t' "${candidates_file}" || cat "${candidates_file}"

  while IFS=$'\t' read -r disk_name zone size_gb disk_type; do
    echo "    Deleting ${disk_name} (${zone}, ${size_gb}GB, ${disk_type})"
    gcloud compute disks delete "${disk_name}" \
      --project="${PROJECT_ID}" \
      --zone="${zone}" \
      --quiet
  done < "${candidates_file}"

  rm -f "${disks_file}" "${candidates_file}"
}

print_config
cleanup_unattached_disks
create_or_reuse_gke_cluster
ensure_storage_classes
install_monitoring
install_target

echo ""
echo "==> Setup complete"
echo "Namespace: ${NS}"
echo "Release:   ${RELEASE}"
echo "Target:    ${TARGET}"

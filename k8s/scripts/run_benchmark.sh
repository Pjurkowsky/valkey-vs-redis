#!/usr/bin/env bash
set -euo pipefail

NS="${NS:-vk}"
RELEASE="${RELEASE:-valkey}"
POD_NAME="memtier-bench"
LOCAL_OUT="${1:-./results/memtier}"
REMOTE_OUT="/work/results/memtier"
SERVICE_ACCOUNT="memtier-sa"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-Always}"
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-300s}"
HELM_CHART_PATH="${HELM_CHART_PATH:-../valkey-helm/valkey}"
VALUES_FILE="${VALUES_FILE:-./k8s/manifests/values.yaml}"
PROVIDER="${PROVIDER:-valkey}"
LOCATION="${LOCATION:-europe-central2}"
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
ARTIFACT_REPO="${ARTIFACT_REPO:-valkey-bench}"
MEMORYSTORE_PRODUCT="${MEMORYSTORE_PRODUCT:-redis}"
MEMORYSTORE_CLUSTER_ID="${MEMORYSTORE_CLUSTER_ID:-${MEMORYSTORE_CLUSTER:-}}"
PORT="${PORT:-${MEMORYSTORE_PORT:-6379}}"
N="${N:-5}"
VALKEY_MAXMEMORY="${VALKEY_MAXMEMORY:-1gb}"
VALKEY_MAXMEMORY_POLICY="${VALKEY_MAXMEMORY_POLICY:-allkeys-lru}"
VALKEY_TLS_SECRET="${VALKEY_TLS_SECRET:-valkey-tls-secret}"
VALKEY_TLS_REQUIRE_CLIENT_CERT="${VALKEY_TLS_REQUIRE_CLIENT_CERT:-false}"

if [[ -n "${MEMTIER_IMAGE:-}" ]]; then
  IMAGE="${MEMTIER_IMAGE}"
elif [[ "${PROVIDER}" == "memorystore" && -n "${PROJECT_ID}" && "${PROJECT_ID}" != "(unset)" ]]; then
  IMAGE="${LOCATION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REPO}/memtier_k8s:1"
else
  IMAGE="memtier_k8s:1"
fi

source "${SCRIPT_DIR}/pod_results.sh"

normalize_bool() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON)
      echo "true"
      ;;
    0|false|FALSE|no|NO|off|OFF)
      echo "false"
      ;;
    *)
      echo ""
      ;;
  esac
}

write_valkey_config() {
  local persistence="$1"
  local config_file="$2"
  local tls_enabled="$3"

  {
    echo "maxmemory ${VALKEY_MAXMEMORY}"
    echo "maxmemory-policy ${VALKEY_MAXMEMORY_POLICY}"

    if [ -n "${persistence}" ]; then
      case "${persistence}" in
        off)
          echo 'save ""'
          echo "appendonly no"
          ;;
        rdb)
          echo "save 60 1"
          echo "appendonly no"
          ;;
        aof)
          echo 'save ""'
          echo "appendonly yes"
          echo "appendfsync everysec"
          ;;
        both)
          echo "save 60 1"
          echo "appendonly yes"
          echo "appendfsync everysec"
          ;;
        *)
          echo "ERROR: VALKEY_PERSISTENCE must be one of: off, rdb, aof, both" >&2
          return 1
          ;;
      esac
    fi

    if [ "${tls_enabled}" = "true" ]; then
      echo "cluster-announce-tls-port 6379"
    fi
  } > "${config_file}"
}

apply_valkey_runtime_flags() {
  local tls_enabled
  local config_file=""
  local helm_args=(
    helm upgrade --install "${RELEASE}" "${HELM_CHART_PATH}"
    -n "${NS}"
    -f "${VALUES_FILE}"
  )

  if [ -n "${VALKEY_TLS:-}" ]; then
    tls_enabled="$(normalize_bool "${VALKEY_TLS}")"
    if [ -z "${tls_enabled}" ]; then
      echo "ERROR: VALKEY_TLS must be true or false" >&2
      return 1
    fi
    helm_args+=(--set "tls.enabled=${tls_enabled}")
    if [ "${tls_enabled}" = "true" ]; then
      helm_args+=(--set "tls.existingSecret=${VALKEY_TLS_SECRET}")
      helm_args+=(--set "tls.requireClientCertificate=${VALKEY_TLS_REQUIRE_CLIENT_CERT}")
    fi
  fi

  if [ -n "${VALKEY_PERSISTENCE:-}" ] || [ "${tls_enabled:-}" = "true" ]; then
    config_file="$(mktemp /tmp/valkey-config.XXXXXX.conf)"
    write_valkey_config "${VALKEY_PERSISTENCE:-}" "${config_file}" "${tls_enabled:-false}"
    helm_args+=(--set-file "valkeyConfig=${config_file}")
  fi

  if [ -n "${VALKEY_PERSISTENCE:-}" ] || [ -n "${VALKEY_TLS:-}" ]; then
    echo "==> Applying Valkey runtime flags..."
    echo "VALKEY_PERSISTENCE=${VALKEY_PERSISTENCE:-unchanged}"
    echo "VALKEY_TLS=${VALKEY_TLS:-unchanged}"
    echo "VALKEY_TLS_SECRET=${VALKEY_TLS_SECRET}"
    "${helm_args[@]}"
    kubectl rollout status "sts/${RELEASE}" -n "${NS}" --timeout="${ROLLOUT_TIMEOUT}"
  fi

  if [ -n "${config_file}" ]; then
    rm -f "${config_file}"
  fi
}

if [[ "${PROVIDER}" != "valkey" && "${PROVIDER}" != "memorystore" ]]; then
  echo "ERROR: PROVIDER must be valkey or memorystore." >&2
  exit 1
fi

PYTHON_BIN="$(command -v python3 || command -v python || true)"

describe_memorystore_json() {
  if [[ "${MEMORYSTORE_PRODUCT}" == "redis" ]]; then
    gcloud redis clusters describe "${MEMORYSTORE_CLUSTER_ID}" \
      --project="${PROJECT_ID}" \
      --region="${LOCATION}" \
      --format=json
  else
    gcloud memorystore instances describe "${MEMORYSTORE_CLUSTER_ID}" \
      --project="${PROJECT_ID}" \
      --location="${LOCATION}" \
      --format=json
  fi
}

discover_memorystore_endpoint() {
  describe_memorystore_json | "${PYTHON_BIN}" -c '
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

for endpoint in doc.get("endpoints") or []:
    port = endpoint.get("port") or 6379
    for connection in endpoint.get("connections") or []:
        candidates = [
            connection.get("pscAutoConnection") or {},
            connection.get("pscConnection") or {},
            connection,
        ]
        for candidate in candidates:
            connection_type = candidate.get("connectionType") or connection.get("connectionType")
            address = candidate.get("address") or candidate.get("ipAddress")
            if connection_type in (None, "", "CONNECTION_TYPE_DISCOVERY") and emit(address, port):
                raise SystemExit(0)

raise SystemExit("Could not find Memorystore discovery endpoint address")
'
}

ensure_memtier_rbac() {
  kubectl apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${SERVICE_ACCOUNT}
  namespace: ${NS}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: workload-resource-editor
  namespace: ${NS}
rules:
- apiGroups: ["apps"]
  resources: ["deployments", "statefulsets"]
  verbs: ["get", "list", "watch", "patch", "update"]
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list", "watch"]
- apiGroups: [""]
  resources: ["events"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: workload-resource-editor-binding
  namespace: ${NS}
subjects:
- kind: ServiceAccount
  name: ${SERVICE_ACCOUNT}
  namespace: ${NS}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: workload-resource-editor
EOF
}

if [[ "${PROVIDER}" == "valkey" ]]; then
  HOST="${HOST:-${RELEASE}.${NS}.svc.cluster.local}"
  BENCHMARKED_SYSTEM="${BENCHMARKED_SYSTEM:-Valkey Cluster in Kubernetes}"
  VARIANT="${VARIANT:-valkey}"
  apply_valkey_runtime_flags
else
  if [[ "${MEMORYSTORE_PRODUCT}" != "redis" && "${MEMORYSTORE_PRODUCT}" != "valkey" ]]; then
    echo "ERROR: MEMORYSTORE_PRODUCT must be redis or valkey." >&2
    exit 1
  fi
  if [[ -z "${HOST:-}" ]]; then
    if [[ -z "${MEMORYSTORE_CLUSTER_ID}" ]]; then
      echo "ERROR: Set MEMORYSTORE_CLUSTER_ID or HOST when PROVIDER=memorystore." >&2
      exit 1
    fi
    if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "(unset)" ]]; then
      echo "ERROR: Could not determine GCP project. Set PROJECT_ID." >&2
      exit 1
    fi
    if [[ -z "${PYTHON_BIN}" ]]; then
      echo "ERROR: python3 or python is required to parse gcloud JSON output." >&2
      exit 1
    fi
    read -r HOST DISCOVERED_PORT < <(discover_memorystore_endpoint)
    if [[ -n "${DISCOVERED_PORT:-}" ]]; then
      PORT="${DISCOVERED_PORT}"
    fi
  fi
  BENCHMARKED_SYSTEM="${BENCHMARKED_SYSTEM:-Memorystore for ${MEMORYSTORE_PRODUCT^} Cluster}"
  VARIANT="${VARIANT:-memorystore_${MEMORYSTORE_PRODUCT}}"
fi

if [[ "${PROVIDER}" == "valkey" ]] && [ "$(normalize_bool "${VALKEY_TLS:-}")" = "true" ] && [ -z "${MEMTIER_TLS:-}" ]; then
  MEMTIER_TLS=true
fi

echo "==> Benchmark launcher configuration"
echo "PROVIDER=${PROVIDER}"
echo "VARIANT=${VARIANT}"
echo "BENCHMARKED_SYSTEM=${BENCHMARKED_SYSTEM}"
echo "NS=${NS}"
echo "RELEASE=${RELEASE}"
echo "HOST=${HOST}"
echo "PORT=${PORT}"
echo "N=${N}"
echo "MEMTIER_IMAGE=${IMAGE}"
echo "IMAGE_PULL_POLICY=${IMAGE_PULL_POLICY}"
echo "LOCAL_OUT=${LOCAL_OUT}"

RUN_ARGS=(
  kubectl run "${POD_NAME}" -n "${NS}"
  --image="${IMAGE}"
  --image-pull-policy="${IMAGE_PULL_POLICY}"
  --restart=Never
  --overrides="{\"apiVersion\":\"v1\",\"spec\":{\"serviceAccountName\":\"${SERVICE_ACCOUNT}\"}}"
)

if [ -n "${MEMTIER_CPUS:-}" ]; then
  RUN_ARGS+=(--env="MEMTIER_CPUS=${MEMTIER_CPUS}")
fi

RUN_ARGS+=(--env="PROVIDER=${PROVIDER}")
RUN_ARGS+=(--env="VARIANT=${VARIANT}")
RUN_ARGS+=(--env="BENCHMARKED_SYSTEM=${BENCHMARKED_SYSTEM}")
RUN_ARGS+=(--env="NS=${NS}")
RUN_ARGS+=(--env="RELEASE=${RELEASE}")
RUN_ARGS+=(--env="HOST=${HOST}")
RUN_ARGS+=(--env="PORT=${PORT}")
RUN_ARGS+=(--env="N=${N}")

for var in \
  MEMTIER_PAYLOADS \
  MEMTIER_RATIOS \
  MEMTIER_THREADS \
  MEMTIER_CLIENTS \
  MEMTIER_PIPELINE \
  MEMTIER_TEST_TIME \
  MEMTIER_KEYS \
  MEMTIER_RANDOM_DATA \
  MEMTIER_TLS \
  MEMTIER_TLS_SKIP_VERIFY \
  MEMTIER_TLS_CACERT \
  MEMTIER_TLS_CERT \
  MEMTIER_TLS_KEY \
  MEMTIER_TLS_SNI \
  MEMTIER_STATSD_PORT
do
  if [ -n "${!var:-}" ]; then
    RUN_ARGS+=(--env="${var}=${!var}")
  fi
done

if [ "${MEMTIER_STATSD_HOST+x}" ]; then
  RUN_ARGS+=(--env="MEMTIER_STATSD_HOST=${MEMTIER_STATSD_HOST}")
fi

RUN_ARGS+=(--env="ROLLOUT_TIMEOUT=${ROLLOUT_TIMEOUT}")
RUN_ARGS+=(--env="MEMTIER_OUTDIR=${REMOTE_OUT}")
RUN_ARGS+=(
  --command --
  /bin/sh -c
  "set +e; /work/run.sh; status=\$?; echo \"\$status\" > '${POD_EXIT_CODE_FILE}'; touch '${POD_DONE_FILE}'; sleep '${POD_HOLD_SECONDS}'"
)

echo "==> Ensuring memtier RBAC exists..."
ensure_memtier_rbac

echo "==> Cleaning up any previous benchmark pod..."
kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found >/dev/null

echo "==> Creating benchmark pod (image=${IMAGE})..."
"${RUN_ARGS[@]}"

echo "==> Waiting for benchmark pod to finish writing results..."
kubectl wait pod/"${POD_NAME}" -n "${NS}" \
  --for=condition=Ready --timeout=60s 2>/dev/null || true

if ! wait_for_pod_marker "${NS}" "${POD_NAME}" "${POD_DONE_FILE}" 7200; then
  echo "==> Benchmark pod did not signal completion."
  print_pod_debug_info "${NS}" "${POD_NAME}"
  exit 1
fi

exit_code="$(read_pod_exit_code "${NS}" "${POD_NAME}" "${POD_EXIT_CODE_FILE}")"
if [[ -z "${exit_code}" || "${exit_code}" != "0" ]]; then
  echo "==> Benchmark command failed with exit code: ${exit_code:-unknown}"
  print_pod_debug_info "${NS}" "${POD_NAME}"
  exit 1
fi

echo "==> Benchmark finished. Copying results to ${LOCAL_OUT}..."
mkdir -p "${LOCAL_OUT}"
kubectl cp "${NS}/${POD_NAME}:${REMOTE_OUT}" "${LOCAL_OUT}"

file_count=$(find "${LOCAL_OUT}" -name '*.json' | wc -l)
echo "==> Copied ${file_count} JSON files to ${LOCAL_OUT}"

echo "==> Cleaning up pod..."
kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found

echo "==> Done. Run analysis with:"
echo "    python cli.py benchmark --input ${LOCAL_OUT} --output-dir ./plots/benchmark"

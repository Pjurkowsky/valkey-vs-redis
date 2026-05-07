#!/usr/bin/env bash
set -euo pipefail

NS="vk"
POD_NAME="memtier-bench"
IMAGE="${MEMTIER_IMAGE:-memtier_k8s:1}"
LOCAL_OUT="${1:-./results/memtier}"
REMOTE_OUT="/work/results/memtier"
SERVICE_ACCOUNT="memtier-sa"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RBAC_MANIFEST="${SCRIPT_DIR}/../memtier_rbac.yaml"
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-300s}"
HELM_CHART_PATH="${HELM_CHART_PATH:-../valkey-helm/valkey}"
VALUES_FILE="${VALUES_FILE:-./k8s/manifests/values.yaml}"
VALKEY_MAXMEMORY="${VALKEY_MAXMEMORY:-1gb}"
VALKEY_MAXMEMORY_POLICY="${VALKEY_MAXMEMORY_POLICY:-allkeys-lru}"
VALKEY_TLS_SECRET="${VALKEY_TLS_SECRET:-valkey-tls-secret}"
VALKEY_TLS_REQUIRE_CLIENT_CERT="${VALKEY_TLS_REQUIRE_CLIENT_CERT:-false}"

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
    helm upgrade --install valkey "${HELM_CHART_PATH}"
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
    kubectl rollout status "sts/valkey" -n "${NS}" --timeout="${ROLLOUT_TIMEOUT}"
  fi

  if [ -n "${config_file}" ]; then
    rm -f "${config_file}"
  fi
}

apply_valkey_runtime_flags

if [ "$(normalize_bool "${VALKEY_TLS:-}")" = "true" ] && [ -z "${MEMTIER_TLS:-}" ]; then
  MEMTIER_TLS=true
fi

RUN_ARGS=(
  kubectl run "${POD_NAME}" -n "${NS}"
  --image="${IMAGE}"
  --restart=Never
  --overrides="{\"apiVersion\":\"v1\",\"spec\":{\"serviceAccountName\":\"${SERVICE_ACCOUNT}\"}}"
)

if [ -n "${MEMTIER_CPUS:-}" ]; then
  RUN_ARGS+=(--env="MEMTIER_CPUS=${MEMTIER_CPUS}")
fi

for var in \
  MEMTIER_PAYLOADS \
  MEMTIER_RATIOS \
  MEMTIER_THREADS \
  MEMTIER_CLIENTS \
  MEMTIER_PIPELINE \
  MEMTIER_TEST_TIME \
  MEMTIER_KEYS \
  MEMTIER_TLS \
  MEMTIER_TLS_SKIP_VERIFY \
  MEMTIER_TLS_CACERT \
  MEMTIER_TLS_CERT \
  MEMTIER_TLS_KEY \
  MEMTIER_TLS_SNI
do
  if [ -n "${!var:-}" ]; then
    RUN_ARGS+=(--env="${var}=${!var}")
  fi
done

RUN_ARGS+=(--env="ROLLOUT_TIMEOUT=${ROLLOUT_TIMEOUT}")
RUN_ARGS+=(--env="MEMTIER_OUTDIR=${REMOTE_OUT}")
RUN_ARGS+=(
  --command --
  /bin/sh -c
  "set +e; /work/run.sh; status=\$?; echo \"\$status\" > '${POD_EXIT_CODE_FILE}'; touch '${POD_DONE_FILE}'; sleep '${POD_HOLD_SECONDS}'"
)

echo "==> Ensuring memtier RBAC exists..."
kubectl apply -f "${RBAC_MANIFEST}"

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

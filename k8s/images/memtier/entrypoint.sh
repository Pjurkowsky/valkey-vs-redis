#!/usr/bin/env bash
set -euo pipefail

N="${N:-5}"

read -r -a CPUs <<< "${MEMTIER_CPUS:-1 2 4}"
read -r -a PAYLOADS <<< "${MEMTIER_PAYLOADS:-1 10 1000}"
read -r -a RATIOS <<< "${MEMTIER_RATIOS:-1:0 0:1 1:1}"

PROVIDER="${PROVIDER:-valkey}"
VARIANT="${VARIANT:-${PROVIDER}}"
BENCHMARKED_SYSTEM="${BENCHMARKED_SYSTEM:-Valkey Cluster in Kubernetes}"
NS="${NS:-vk}"
STS="${RELEASE:-valkey}"
CONTAINER="${CONTAINER:-valkey}"

HOST="${HOST:-valkey.vk.svc.cluster.local}"
PORT="${PORT:-6379}"
THREADS="${MEMTIER_THREADS:-4}"
CLIENTS="${MEMTIER_CLIENTS:-25}"
PIPELINE="${MEMTIER_PIPELINE:-10}"
TEST_TIME="${MEMTIER_TEST_TIME:-300}"
KEYS="${MEMTIER_KEYS:-1000000}"
MEMTIER_RANDOM_DATA="${MEMTIER_RANDOM_DATA:-false}"
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-300s}"
MEMTIER_TLS="${MEMTIER_TLS:-false}"
MEMTIER_TLS_SKIP_VERIFY="${MEMTIER_TLS_SKIP_VERIFY:-true}"
MEMTIER_TLS_CACERT="${MEMTIER_TLS_CACERT:-}"
MEMTIER_TLS_CERT="${MEMTIER_TLS_CERT:-}"
MEMTIER_TLS_KEY="${MEMTIER_TLS_KEY:-}"
MEMTIER_TLS_SNI="${MEMTIER_TLS_SNI:-}"

STATSD_HOST="${MEMTIER_STATSD_HOST-statsd-exporter.monitoring.svc.cluster.local}"
STATSD_PORT="${MEMTIER_STATSD_PORT:-9125}"

OUTDIR="${MEMTIER_OUTDIR:-./results/memtier}"
mkdir -p "${OUTDIR}"

TLS_ARGS=()
case "${MEMTIER_TLS}" in
  1|true|TRUE|yes|YES)
    TLS_ARGS+=(--tls)
    case "${MEMTIER_TLS_SKIP_VERIFY}" in
      1|true|TRUE|yes|YES)
        TLS_ARGS+=(--tls-skip-verify)
        ;;
    esac
    if [ -n "${MEMTIER_TLS_CACERT}" ]; then
      TLS_ARGS+=(--cacert="${MEMTIER_TLS_CACERT}")
    fi
    if [ -n "${MEMTIER_TLS_CERT}" ]; then
      TLS_ARGS+=(--cert="${MEMTIER_TLS_CERT}")
    fi
    if [ -n "${MEMTIER_TLS_KEY}" ]; then
      TLS_ARGS+=(--key="${MEMTIER_TLS_KEY}")
    fi
    if [ -n "${MEMTIER_TLS_SNI}" ]; then
      TLS_ARGS+=(--sni="${MEMTIER_TLS_SNI}")
    fi
    ;;
esac

STATSD_ARGS=()
if [ -n "${STATSD_HOST}" ]; then
  STATSD_ARGS+=(--statsd-host="${STATSD_HOST}")
  STATSD_ARGS+=(--statsd-port="${STATSD_PORT}")
fi

RANDOM_DATA_ARGS=()
case "${MEMTIER_RANDOM_DATA}" in
  1|true|TRUE|yes|YES|on|ON)
    RANDOM_DATA_ARGS+=(--random-data)
    ;;
esac

annotate_result() {
  local file="$1"
  local cpu="$2"
  local payload="$3"
  local ratio="$4"
  local tag="$5"

  if ! command -v python3 >/dev/null 2>&1; then
    echo "WARN: python3 not found; leaving ${file} without benchmark metadata"
    return 0
  fi

  python3 - "${file}" "${cpu}" "${payload}" "${ratio}" "${tag}" <<'PY'
import json
import os
import sys

path, cpu, payload, ratio, tag = sys.argv[1:]

def int_or_str(value):
    try:
        return int(value)
    except ValueError:
        return value

def int_or_default(value, default):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def bool_from_env(name, default=False):
    value = os.environ.get(name)
    if value is None:
        return default
    return value.lower() in ("1", "true", "yes", "on")

with open(path, "r", encoding="utf-8") as fh:
    doc = json.load(fh)

doc["variant"] = os.environ.get("VARIANT", os.environ.get("PROVIDER", "valkey"))
doc["provider"] = os.environ.get("PROVIDER", "valkey")
doc["benchmarked_system"] = os.environ.get("BENCHMARKED_SYSTEM", "")
doc["benchmark"] = "memtier_performance"
doc["target"] = {
    "host": os.environ.get("HOST", ""),
    "port": int_or_default(os.environ.get("PORT"), 6379),
}
doc["workload"] = {
    "tag": tag,
    "cpu": int_or_str(cpu),
    "payload_kb": int_or_default(payload, 0),
    "ratio": ratio,
    "threads": int_or_default(os.environ.get("MEMTIER_THREADS"), 4),
    "clients": int_or_default(os.environ.get("MEMTIER_CLIENTS"), 25),
    "pipeline": int_or_default(os.environ.get("MEMTIER_PIPELINE"), 10),
    "test_time_s": int_or_default(os.environ.get("MEMTIER_TEST_TIME"), 300),
    "key_maximum": int_or_default(os.environ.get("MEMTIER_KEYS"), 1000000),
    "run_count": int_or_default(os.environ.get("N"), 5),
    "random_data": bool_from_env("MEMTIER_RANDOM_DATA"),
}

with open(path, "w", encoding="utf-8") as fh:
    json.dump(doc, fh, indent=2)
    fh.write("\n")
PY
}

run_memtier_case() {
  local cpu="$1"
  local payload="$2"
  local ratio="$3"
  local ratio_safe="${ratio/:/-}"
  local tag="${cpu}_${payload}_${ratio_safe}"
  local base="${OUTDIR}/${tag}"
  local data_size_bytes=$((payload * 1024))

  echo "cpu=${cpu} payload=${payload}KB ratio=${ratio}"
  memtier_benchmark \
    --server="${HOST}" --port="${PORT}" \
    --protocol=redis \
    --cluster-mode \
    "${TLS_ARGS[@]}" \
    --threads="${THREADS}" --clients="${CLIENTS}" \
    --pipeline="${PIPELINE}" \
    --test-time="${TEST_TIME}" \
    --key-maximum="${KEYS}" \
    --data-size="${data_size_bytes}" \
    "${RANDOM_DATA_ARGS[@]}" \
    --ratio="${ratio}" \
    --json-out-file "${base}.json" \
    --run-count "${N}" \
    --print-all-runs \
    --print-percentiles="50,95,99,99.9" \
    "${STATSD_ARGS[@]}" \
    --statsd-run-label="${tag}"

  annotate_result "${base}.json" "${cpu}" "${payload}" "${ratio}" "${tag}"
}

echo "==> Benchmark configuration"
echo "PROVIDER=${PROVIDER}"
echo "VARIANT=${VARIANT}"
echo "BENCHMARKED_SYSTEM=${BENCHMARKED_SYSTEM}"
echo "HOST=${HOST}"
echo "PORT=${PORT}"
echo "NS=${NS}"
echo "RELEASE=${STS}"
echo "N=${N}"
echo "MEMTIER_CPUS=${CPUs[*]}"
echo "MEMTIER_PAYLOADS=${PAYLOADS[*]}"
echo "MEMTIER_RATIOS=${RATIOS[*]}"
echo "MEMTIER_THREADS=${THREADS}"
echo "MEMTIER_CLIENTS=${CLIENTS}"
echo "MEMTIER_PIPELINE=${PIPELINE}"
echo "MEMTIER_TEST_TIME=${TEST_TIME}"
echo "MEMTIER_KEYS=${KEYS}"
echo "MEMTIER_RANDOM_DATA=${MEMTIER_RANDOM_DATA}"
echo "MEMTIER_RANDOM_DATA_ARGS=${RANDOM_DATA_ARGS[*]:-}"
echo "MEMTIER_TLS=${MEMTIER_TLS}"
echo "MEMTIER_TLS_SKIP_VERIFY=${MEMTIER_TLS_SKIP_VERIFY}"
echo "MEMTIER_TLS_ARGS=${TLS_ARGS[*]:-}"
echo "MEMTIER_STATSD_HOST=${STATSD_HOST}"
echo "MEMTIER_STATSD_PORT=${STATSD_PORT}"
echo "ROLLOUT_TIMEOUT=${ROLLOUT_TIMEOUT}"

if [ "${PROVIDER}" = "memorystore" ]; then
  echo "==> Managed target detected; skipping Kubernetes StatefulSet CPU sweep"
  for payload in "${PAYLOADS[@]}"; do
    for ratio in "${RATIOS[@]}"; do
      run_memtier_case "0" "${payload}" "${ratio}"
    done
  done
  exit 0
fi

for cpu in "${CPUs[@]}"; do
  echo "==> Setting CPU for ${STS}/${CONTAINER} to ${cpu} vCPU (requests+limits)"
  kubectl set resources "sts/${STS}" -n "${NS}" -c "${CONTAINER}" \
    --requests="cpu=${cpu}" --limits="cpu=${cpu}"

  echo "==> Waiting for rollout..."
  if ! kubectl rollout status "sts/${STS}" -n "${NS}" --timeout="${ROLLOUT_TIMEOUT}"; then
    echo "ERROR: rollout did not complete within ${ROLLOUT_TIMEOUT}"
    echo "INFO: current pod states:"
    kubectl get pods -n "${NS}" -o wide || true

    pending_pod="$(
      kubectl get pods -n "${NS}" \
        --field-selector=status.phase=Pending \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
    )"
    if [ -n "${pending_pod}" ]; then
      echo "INFO: describe for pending pod ${pending_pod}:"
      kubectl describe pod "${pending_pod}" -n "${NS}" || true
    fi
    exit 1
  fi

  actual=$(kubectl get sts/"${STS}" -n "${NS}" -o jsonpath='{.spec.template.spec.containers[0].resources.limits.cpu}')
  if [ "$actual" != "$cpu" ]; then
    echo "ERROR: expected cpu=$cpu but got cpu=$actual"
    exit 1
  fi
  echo "INFO: cpu=$cpu set successfully"

  for payload in "${PAYLOADS[@]}"; do
    for ratio in "${RATIOS[@]}"; do
      run_memtier_case "${cpu}" "${payload}" "${ratio}"
    done
  done
done

#!/usr/bin/env bash
set -euo pipefail

N=5

read -r -a CPUs <<< "${MEMTIER_CPUS:-1 2 4}"
read -r -a PAYLOADS <<< "${MEMTIER_PAYLOADS:-1 10 1000}"
read -r -a RATIOS <<< "${MEMTIER_RATIOS:-0:1 1:0 1:1}"

NS="vk"
STS="valkey"
CONTAINER="valkey"

HOST="valkey.vk.svc.cluster.local"
PORT=6379
THREADS="${MEMTIER_THREADS:-4}"
CLIENTS="${MEMTIER_CLIENTS:-25}"
PIPELINE="${MEMTIER_PIPELINE:-10}"
TEST_TIME="${MEMTIER_TEST_TIME:-300}"
KEYS="${MEMTIER_KEYS:-1000000}"
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-300s}"
MEMTIER_TLS="${MEMTIER_TLS:-false}"
MEMTIER_TLS_SKIP_VERIFY="${MEMTIER_TLS_SKIP_VERIFY:-true}"
MEMTIER_TLS_CACERT="${MEMTIER_TLS_CACERT:-}"
MEMTIER_TLS_CERT="${MEMTIER_TLS_CERT:-}"
MEMTIER_TLS_KEY="${MEMTIER_TLS_KEY:-}"
MEMTIER_TLS_SNI="${MEMTIER_TLS_SNI:-}"

STATSD_HOST="statsd-exporter.monitoring.svc.cluster.local"
STATSD_PORT=9125

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

echo "==> Benchmark configuration"
echo "MEMTIER_CPUS=${CPUs[*]}"
echo "MEMTIER_PAYLOADS=${PAYLOADS[*]}"
echo "MEMTIER_RATIOS=${RATIOS[*]}"
echo "MEMTIER_THREADS=${THREADS}"
echo "MEMTIER_CLIENTS=${CLIENTS}"
echo "MEMTIER_PIPELINE=${PIPELINE}"
echo "MEMTIER_TEST_TIME=${TEST_TIME}"
echo "MEMTIER_KEYS=${KEYS}"
echo "MEMTIER_TLS=${MEMTIER_TLS}"
echo "MEMTIER_TLS_SKIP_VERIFY=${MEMTIER_TLS_SKIP_VERIFY}"
echo "MEMTIER_TLS_ARGS=${TLS_ARGS[*]:-}"
echo "ROLLOUT_TIMEOUT=${ROLLOUT_TIMEOUT}"

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
      data_size_bytes=$((payload * 1024))  
      for ratio in "${RATIOS[@]}"; do
      ratio_safe="${ratio/:/-}"   # 1:1 -> 1-1
        TAG="${cpu}_${payload}_${ratio_safe}"
        BASE="${OUTDIR}/${TAG}"
        echo "cpu=$cpu payload=${payload}KB ratio=$ratio"
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
          --ratio="${ratio}" \
          --json-out-file "${BASE}.json" \
          --run-count "${N}" \
          --print-all-runs \
          --print-percentiles="50,95,99,99.9" \
          --statsd-host="${STATSD_HOST}" \
          --statsd-port="${STATSD_PORT}" \
          --statsd-run-label="${TAG}" 

    done
  done
done

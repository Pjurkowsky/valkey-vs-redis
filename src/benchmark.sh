#!/usr/bin/env bash
N=5

CPUs=(1 2 4)
PAYLOADS=(1 10 1000)
RATIOS=("0:1" "1:0" "1:1")

NS="vk"
STS="valkey"
CONTAINER="valkey"

HOST="valkey.vk.svc.cluster.local"
PORT=6379
THREADS=4
CLIENTS=16
TEST_TIME=10
KEYS=100000

OUTDIR="./results_memtier"
mkdir -p "${OUTDIR}"

for cpu in "${CPUs[@]}"; do
  echo "==> Setting CPU for ${STS}/${CONTAINER} to ${cpu} vCPU (requests+limits)"
  kubectl set resources "sts/${STS}" -n "${NS}" -c "${CONTAINER}" \
    --requests="cpu=${cpu}" --limits="cpu=${cpu}"

  echo "==> Waiting for rollout..."
  kubectl rollout status "sts/${STS}" -n "${NS}"
    
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
          --threads="${THREADS}" --clients="${CLIENTS}" \
          --test-time="${TEST_TIME}" \
          --key-maximum="${KEYS}" \
          --data-size="${data_size_bytes}" \
          --ratio="${ratio}" \
          --json-out-file "${BASE}.json" \
          --run-count "${N}" \
          --print-all-runs \
          --print-percentiles="50,95,99,99.9"

    done
  done
done
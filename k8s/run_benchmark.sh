#!/usr/bin/env bash
set -euo pipefail

NS="vk"
POD_NAME="memtier-bench"
IMAGE="${MEMTIER_IMAGE:-memtier_k8s:1}"
LOCAL_OUT="${1:-./results_memtier}"
REMOTE_OUT="/work/results_memtier"

echo "==> Creating benchmark pod (image=${IMAGE})..."
kubectl run "${POD_NAME}" -n "${NS}" \
  --image="${IMAGE}" \
  --restart=Never \
  --command -- /work/run.sh

echo "==> Waiting for benchmark pod to complete..."
kubectl wait pod/"${POD_NAME}" -n "${NS}" \
  --for=condition=Ready --timeout=60s 2>/dev/null || true

kubectl wait pod/"${POD_NAME}" -n "${NS}" \
  --for=jsonpath='{.status.phase}'=Succeeded --timeout=7200s

echo "==> Benchmark finished. Copying results to ${LOCAL_OUT}..."
mkdir -p "${LOCAL_OUT}"
kubectl cp "${NS}/${POD_NAME}:${REMOTE_OUT}" "${LOCAL_OUT}"

file_count=$(find "${LOCAL_OUT}" -name '*.json' | wc -l)
echo "==> Copied ${file_count} JSON files to ${LOCAL_OUT}"

echo "==> Cleaning up pod..."
kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found

echo "==> Done. Run analysis with:"
echo "    python main.py --input ${LOCAL_OUT} --output-dir ./benchmark_plots"

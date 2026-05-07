#!/usr/bin/env bash
set -euo pipefail

NS="vk"
LOCAL_OUT="${1:-./results/memtier}"
REMOTE_OUT="/work/results/memtier"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Cleaning up any previous chaos..."
kubectl delete networkchaos -n chaos-mesh --all
kubectl delete podchaos -n chaos-mesh --all
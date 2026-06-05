#!/usr/bin/env bash
set -euo pipefail

CHAOS_NAMESPACE="${CHAOS_NAMESPACE:-chaos-mesh}"
CHAOS_RELEASE="${CHAOS_RELEASE:-chaos-mesh}"
CHAOS_RUNTIME="${CHAOS_RUNTIME:-containerd}"
CHAOS_SOCKET_PATH="${CHAOS_SOCKET_PATH:-/run/containerd/containerd.sock}"
CHAOS_HELM_TIMEOUT="${CHAOS_HELM_TIMEOUT:-5m}"

cat <<EOF
==> Chaos Mesh install configuration
CHAOS_NAMESPACE=${CHAOS_NAMESPACE}
CHAOS_RELEASE=${CHAOS_RELEASE}
CHAOS_RUNTIME=${CHAOS_RUNTIME}
CHAOS_SOCKET_PATH=${CHAOS_SOCKET_PATH}
CHAOS_HELM_TIMEOUT=${CHAOS_HELM_TIMEOUT}
EOF

helm repo add chaos-mesh https://charts.chaos-mesh.org
helm repo update

helm upgrade --install "${CHAOS_RELEASE}" chaos-mesh/chaos-mesh \
    --namespace "${CHAOS_NAMESPACE}" --create-namespace \
    --set "chaosDaemon.runtime=${CHAOS_RUNTIME}" \
    --set "chaosDaemon.socketPath=${CHAOS_SOCKET_PATH}" \
    --wait \
    --timeout="${CHAOS_HELM_TIMEOUT}"

echo "Waiting for Chaos Mesh pods to be ready..."
kubectl wait --for=condition=Ready pods --all -n "${CHAOS_NAMESPACE}" --timeout=120s

echo "Chaos Mesh installed. Dashboard available via:"
echo "  kubectl port-forward -n ${CHAOS_NAMESPACE} svc/chaos-dashboard 2333:2333"

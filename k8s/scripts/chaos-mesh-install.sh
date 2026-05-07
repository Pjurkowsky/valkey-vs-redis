#!/usr/bin/env bash
set -euo pipefail

helm repo add chaos-mesh https://charts.chaos-mesh.org
helm repo update

helm install chaos-mesh chaos-mesh/chaos-mesh \
    --namespace chaos-mesh --create-namespace \
    --set chaosDaemon.runtime=docker \
    --set chaosDaemon.socketPath=/var/run/docker.sock

echo "Waiting for Chaos Mesh pods to be ready..."
kubectl wait --for=condition=Ready pods --all -n chaos-mesh --timeout=120s

echo "Chaos Mesh installed. Dashboard available via:"
echo "  kubectl port-forward -n chaos-mesh svc/chaos-dashboard 2333:2333"

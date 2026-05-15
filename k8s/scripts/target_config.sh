#!/usr/bin/env bash
# Shared target configuration for benchmark scripts.
# Source this file after setting TARGET (valkey or redis).
# All variables use defaults that can be overridden via env.

TARGET="${TARGET:-valkey}"

case "${TARGET}" in
  valkey)
    TC_HOST="${TC_HOST:-valkey.vk.svc.cluster.local}"
    TC_STS="${TC_STS:-valkey}"
    TC_CONTAINER="${TC_CONTAINER:-valkey}"
    TC_CLI="${TC_CLI:-valkey-cli}"
    TC_POD_PREFIX="${TC_POD_PREFIX:-valkey-}"
    TC_HEADLESS_SVC="${TC_HEADLESS_SVC:-valkey-headless}"
    TC_HELM_RELEASE="${TC_HELM_RELEASE:-valkey}"
    TC_HELM_CHART="${HELM_CHART_PATH:-../valkey-helm/valkey}"
    TC_VALUES_FILE="${VALUES_FILE:-./k8s/manifests/values.yaml}"
    TC_CONFIG_KEY="${TC_CONFIG_KEY:-valkeyConfig}"
    TC_APP_NAME_LABEL="${TC_APP_NAME_LABEL:-valkey}"
    TC_APP_INSTANCE_LABEL="${TC_APP_INSTANCE_LABEL:-valkey}"
    TC_PROBE_IMAGE="${PROBE_IMAGE:-docker.io/valkey/valkey:9.0.1}"
    TC_SCALE_KEY="${TC_SCALE_KEY:-cluster.shards}"
    ;;
  redis)
    TC_HOST="${TC_HOST:-redis-redis-cluster.vk.svc.cluster.local}"
    TC_STS="${TC_STS:-redis-redis-cluster}"
    TC_CONTAINER="${TC_CONTAINER:-redis-cluster}"
    TC_CLI="${TC_CLI:-redis-cli}"
    TC_POD_PREFIX="${TC_POD_PREFIX:-redis-redis-cluster-}"
    TC_HEADLESS_SVC="${TC_HEADLESS_SVC:-redis-redis-cluster-headless}"
    TC_HELM_RELEASE="${TC_HELM_RELEASE:-redis}"
    TC_HELM_CHART="${HELM_CHART_PATH:-oci://registry-1.docker.io/bitnamicharts/redis-cluster}"
    TC_VALUES_FILE="${VALUES_FILE:-./k8s/manifests/redis-values.yaml}"
    TC_CONFIG_KEY="${TC_CONFIG_KEY:-redis.configmap}"
    TC_APP_NAME_LABEL="${TC_APP_NAME_LABEL:-redis-cluster}"
    TC_APP_INSTANCE_LABEL="${TC_APP_INSTANCE_LABEL:-redis}"
    TC_PROBE_IMAGE="${PROBE_IMAGE:-docker.io/redis:7.2}"
    TC_SCALE_KEY="${TC_SCALE_KEY:-cluster.nodes}"
    ;;
  *)
    echo "ERROR: TARGET must be 'valkey' or 'redis', got '${TARGET}'" >&2
    exit 1
    ;;
esac

TC_NS="${TC_NS:-vk}"
TC_PORT="${TC_PORT:-6379}"

tc_pod_name() {
  local ordinal="$1"
  echo "${TC_POD_PREFIX}${ordinal}"
}

tc_admin_pod() {
  tc_pod_name 0
}

tc_admin_host() {
  echo "$(tc_pod_name 0).${TC_HEADLESS_SVC}.${TC_NS}.svc.cluster.local"
}

tc_scale_value() {
  local shards="$1"
  if [[ "${TARGET}" == "redis" ]]; then
    local replicas="${REPLICAS_PER_SHARD:-1}"
    echo "$((shards * (1 + replicas)))"
  else
    echo "${shards}"
  fi
}

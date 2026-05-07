#!/usr/bin/env bash

POD_DONE_FILE="${POD_DONE_FILE:-/tmp/benchmark.done}"
POD_EXIT_CODE_FILE="${POD_EXIT_CODE_FILE:-/tmp/benchmark.exitcode}"
POD_HOLD_SECONDS="${POD_HOLD_SECONDS:-3600}"

wait_for_pod_marker() {
  local namespace="$1"
  local pod_name="$2"
  local marker_file="$3"
  local timeout_seconds="$4"
  local deadline=$((SECONDS + timeout_seconds))

  while (( SECONDS < deadline )); do
    if kubectl exec "${pod_name}" -n "${namespace}" -- test -f "${marker_file}" >/dev/null 2>&1; then
      return 0
    fi

    local phase
    phase="$(kubectl get pod "${pod_name}" -n "${namespace}" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    if [[ "${phase}" == "Succeeded" || "${phase}" == "Failed" ]]; then
      break
    fi

    sleep 2
  done

  return 1
}

read_pod_exit_code() {
  local namespace="$1"
  local pod_name="$2"
  local exit_code_file="$3"

  kubectl exec "${pod_name}" -n "${namespace}" -- cat "${exit_code_file}" 2>/dev/null | tr -d '[:space:]'
}

print_pod_debug_info() {
  local namespace="$1"
  local pod_name="$2"

  kubectl get pod "${pod_name}" -n "${namespace}" -o wide || true
  kubectl logs "${pod_name}" -n "${namespace}" || true
}

#!/usr/bin/env bash
set -euo pipefail

N="${N:-5}"

read -r -a CPUs <<< "${MEMTIER_CPUS:-1 2 4}"
read -r -a PAYLOADS <<< "${MEMTIER_PAYLOADS:-1 10 1000}"
read -r -a RATIOS <<< "${MEMTIER_RATIOS:-1:0 0:1 1:1}"

PROVIDER="${PROVIDER:-valkey}"
case "${PROVIDER}" in
  redis|redis72)
    PROVIDER="redis72"
    DEFAULT_NS="redis"
    DEFAULT_RELEASE="redis72"
    DEFAULT_BENCHMARKED_SYSTEM="Redis 7.2 Cluster in Kubernetes"
    ;;
  memorystore)
    DEFAULT_NS="vk"
    DEFAULT_RELEASE="memorystore"
    DEFAULT_CONTAINER="valkey"
    DEFAULT_BENCHMARKED_SYSTEM="Memorystore Cluster"
    ;;
  *)
    DEFAULT_NS="vk"
    DEFAULT_RELEASE="valkey"
    DEFAULT_CONTAINER="valkey"
    DEFAULT_BENCHMARKED_SYSTEM="Valkey Cluster in Kubernetes"
    ;;
esac

VARIANT="${VARIANT:-${PROVIDER}}"
BENCHMARKED_SYSTEM="${BENCHMARKED_SYSTEM:-${DEFAULT_BENCHMARKED_SYSTEM}}"
NS="${NS:-${DEFAULT_NS}}"
RELEASE="${RELEASE:-${DEFAULT_RELEASE}}"
if [ "${PROVIDER}" = "redis72" ]; then
  STS="${STS:-${RELEASE}-redis-cluster}"
  HOST="${HOST:-${STS}.${NS}.svc.cluster.local}"
  CONTAINER="${CONTAINER:-${STS}}"
else
  STS="${STS:-${RELEASE}}"
  HOST="${HOST:-${RELEASE}.${NS}.svc.cluster.local}"
  CONTAINER="${CONTAINER:-${DEFAULT_CONTAINER}}"
fi

PORT="${PORT:-6379}"
THREADS="${MEMTIER_THREADS:-4}"
CLIENTS="${MEMTIER_CLIENTS:-25}"
PIPELINE="${MEMTIER_PIPELINE:-10}"
TEST_TIME="${MEMTIER_TEST_TIME:-120}"
KEYS="${MEMTIER_KEYS:-1000000}"
TARGET_DATASET_MB="${MEMTIER_TARGET_DATASET_MB:-1536}"
VALUE_OVERHEAD_BYTES="${MEMTIER_VALUE_OVERHEAD_BYTES:-256}"
MEMTIER_RANDOM_DATA="${MEMTIER_RANDOM_DATA:-true}"
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-300s}"
MEMTIER_TLS="${MEMTIER_TLS:-false}"
MEMTIER_TLS_SKIP_VERIFY="${MEMTIER_TLS_SKIP_VERIFY:-true}"
MEMTIER_TLS_CACERT="${MEMTIER_TLS_CACERT:-}"
MEMTIER_TLS_CERT="${MEMTIER_TLS_CERT:-}"
MEMTIER_TLS_KEY="${MEMTIER_TLS_KEY:-}"
MEMTIER_TLS_SNI="${MEMTIER_TLS_SNI:-}"
if [ "${PROVIDER}" = "memorystore" ]; then
  RESET_BETWEEN_RUNS="${RESET_BETWEEN_RUNS:-false}"
  WARMUP_BEFORE_RUNS="${WARMUP_BEFORE_RUNS:-false}"
else
  RESET_BETWEEN_RUNS="${RESET_BETWEEN_RUNS:-true}"
  WARMUP_BEFORE_RUNS="${WARMUP_BEFORE_RUNS:-true}"
fi
RESET_COMMAND_TIMEOUT="${RESET_COMMAND_TIMEOUT:-10}"
MEMTIER_WARMUP_THREADS="${MEMTIER_WARMUP_THREADS:-1}"
MEMTIER_WARMUP_CLIENTS="${MEMTIER_WARMUP_CLIENTS:-1}"
MEMTIER_WARMUP_PIPELINE="${MEMTIER_WARMUP_PIPELINE:-${PIPELINE}}"
MEMTIER_WARMUP_RATIO="${MEMTIER_WARMUP_RATIO:-1:0}"
MEMTIER_WARMUP_KEY_PATTERN="${MEMTIER_WARMUP_KEY_PATTERN:-S:S}"

export N PROVIDER VARIANT BENCHMARKED_SYSTEM NS RELEASE STS CONTAINER HOST PORT ROLLOUT_TIMEOUT
export MEMTIER_THREADS="${THREADS}"
export MEMTIER_CLIENTS="${CLIENTS}"
export MEMTIER_PIPELINE="${PIPELINE}"
export MEMTIER_TEST_TIME="${TEST_TIME}"
export MEMTIER_KEYS="${KEYS}"
export MEMTIER_TARGET_DATASET_MB="${TARGET_DATASET_MB}"
export MEMTIER_VALUE_OVERHEAD_BYTES="${VALUE_OVERHEAD_BYTES}"
export MEMTIER_RANDOM_DATA
export MEMTIER_TLS MEMTIER_TLS_SKIP_VERIFY MEMTIER_TLS_CACERT MEMTIER_TLS_CERT MEMTIER_TLS_KEY MEMTIER_TLS_SNI
export RESET_BETWEEN_RUNS RESET_COMMAND_TIMEOUT WARMUP_BEFORE_RUNS
export MEMTIER_WARMUP_THREADS MEMTIER_WARMUP_CLIENTS MEMTIER_WARMUP_PIPELINE
export MEMTIER_WARMUP_RATIO MEMTIER_WARMUP_KEY_PATTERN

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
  local key_maximum="$6"

  if ! command -v python3 >/dev/null 2>&1; then
    echo "WARN: python3 not found; leaving ${file} without benchmark metadata"
    return 0
  fi

  python3 - "${file}" "${cpu}" "${payload}" "${ratio}" "${tag}" "${key_maximum}" <<'PY'
import json
import os
import sys

path, cpu, payload, ratio, tag, key_maximum = sys.argv[1:]

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
    "test_time_s": int_or_default(os.environ.get("MEMTIER_TEST_TIME"), 120),
    "key_maximum": int_or_default(key_maximum, 0),
    "configured_key_maximum": int_or_default(os.environ.get("MEMTIER_KEYS"), 1000000),
    "target_dataset_mb": int_or_default(os.environ.get("MEMTIER_TARGET_DATASET_MB"), 1536),
    "value_overhead_bytes": int_or_default(os.environ.get("MEMTIER_VALUE_OVERHEAD_BYTES"), 256),
    "run_count": int_or_default(os.environ.get("N"), 5),
    "random_data": bool_from_env("MEMTIER_RANDOM_DATA"),
    "reset_between_runs": bool_from_env("RESET_BETWEEN_RUNS"),
    "warmup_before_runs": bool_from_env("WARMUP_BEFORE_RUNS"),
    "warmup_ratio": os.environ.get("MEMTIER_WARMUP_RATIO", "1:0"),
    "warmup_key_pattern": os.environ.get("MEMTIER_WARMUP_KEY_PATTERN", "S:S"),
}
doc["artifacts"] = {
    "raw_run_dir": f"raw/{tag}",
}

with open(path, "w", encoding="utf-8") as fh:
    json.dump(doc, fh, indent=2)
    fh.write("\n")
PY
}

effective_key_maximum() {
  local payload="$1"

  python3 - "${payload}" <<'PY'
import os
import sys

payload_kb = int(sys.argv[1])
configured_keys = int(os.environ.get("MEMTIER_KEYS", "1000000"))
target_mb = float(os.environ.get("MEMTIER_TARGET_DATASET_MB", "1536"))
overhead = int(os.environ.get("MEMTIER_VALUE_OVERHEAD_BYTES", "256"))

if target_mb <= 0:
    print(configured_keys)
    raise SystemExit(0)

payload_bytes = payload_kb * 1024
bytes_per_key = max(payload_bytes + overhead, 1)
target_bytes = int(target_mb * 1024 * 1024)
safe_keys = max(target_bytes // bytes_per_key, 1)
print(max(min(configured_keys, safe_keys), 1))
PY
}

reset_cluster_data() {
  local cpu="$1"
  local payload="$2"
  local ratio="$3"
  local run_id="$4"

  case "${RESET_BETWEEN_RUNS}" in
    1|true|TRUE|yes|YES|on|ON)
      ;;
    *)
      return 0
      ;;
  esac

  echo "==> Resetting target data before run ${run_id} (cpu=${cpu}, payload=${payload}KB, ratio=${ratio})"
  python3 - <<'PY'
import os
import socket
import ssl
import sys

host = os.environ.get("HOST", "valkey.vk.svc.cluster.local")
port = int(os.environ.get("PORT", "6379"))
timeout = float(os.environ.get("RESET_COMMAND_TIMEOUT", "10"))
tls = os.environ.get("MEMTIER_TLS", "false").lower() in ("1", "true", "yes", "on")
skip_verify = os.environ.get("MEMTIER_TLS_SKIP_VERIFY", "true").lower() in ("1", "true", "yes", "on")
tls_sni = os.environ.get("MEMTIER_TLS_SNI")


class RespError(Exception):
    pass


def connect(target_host, target_port):
    raw = socket.create_connection((target_host, int(target_port)), timeout=timeout)
    raw.settimeout(timeout)
    if not tls:
        return raw

    if skip_verify:
        ctx = ssl._create_unverified_context()
    else:
        ctx = ssl.create_default_context(cafile=os.environ.get("MEMTIER_TLS_CACERT") or None)
    return ctx.wrap_socket(raw, server_hostname=tls_sni or target_host)


def encode_command(*parts):
    chunks = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        if isinstance(part, str):
            part = part.encode()
        chunks.append(f"${len(part)}\r\n".encode())
        chunks.append(part)
        chunks.append(b"\r\n")
    return b"".join(chunks)


def read_line(sock):
    chunks = []
    while True:
        char = sock.recv(1)
        if not char:
            raise EOFError("connection closed while reading RESP line")
        chunks.append(char)
        if len(chunks) >= 2 and chunks[-2:] == [b"\r", b"\n"]:
            return b"".join(chunks[:-2])


def read_resp(sock):
    prefix = sock.recv(1)
    if not prefix:
        raise EOFError("connection closed while reading RESP prefix")
    if prefix == b"+":
        return read_line(sock).decode()
    if prefix == b"-":
        raise RespError(read_line(sock).decode())
    if prefix == b":":
        return int(read_line(sock))
    if prefix == b"$":
        size = int(read_line(sock))
        if size == -1:
            return None
        data = b""
        while len(data) < size:
            chunk = sock.recv(size - len(data))
            if not chunk:
                raise EOFError("connection closed while reading bulk string")
            data += chunk
        if sock.recv(2) != b"\r\n":
            raise ValueError("invalid RESP bulk terminator")
        return data.decode()
    if prefix == b"*":
        count = int(read_line(sock))
        if count == -1:
            return None
        return [read_resp(sock) for _ in range(count)]
    raise ValueError(f"unknown RESP prefix: {prefix!r}")


def command(target_host, target_port, *parts):
    with connect(target_host, target_port) as sock:
        sock.sendall(encode_command(*parts))
        return read_resp(sock)


def metadata_value(node, key):
    for idx, value in enumerate(node):
        if value == key and idx + 1 < len(node):
            return node[idx + 1]
        if isinstance(value, list):
            found = metadata_value(value, key)
            if found:
                return found
    return None


def endpoint_host(node):
    advertised = node[0] if node else None
    if advertised:
        return advertised
    return metadata_value(node, "hostname") or metadata_value(node, "ip")


slots = command(host, port, "CLUSTER", "SLOTS")
masters = []
seen = set()
for slot in slots:
    if len(slot) < 3:
        continue
    master = slot[2]
    master_host = endpoint_host(master)
    master_port = master[1] if len(master) > 1 else port
    if not master_host:
        raise SystemExit(f"could not determine master endpoint from CLUSTER SLOTS entry: {master!r}")
    key = (master_host, int(master_port))
    if key not in seen:
        seen.add(key)
        masters.append(key)

if not masters:
    raise SystemExit("CLUSTER SLOTS returned no masters")

for master_host, master_port in masters:
    try:
        command(master_host, master_port, "FLUSHALL", "SYNC")
    except RespError as exc:
        if "syntax" not in str(exc).lower():
            raise
        command(master_host, master_port, "FLUSHALL")
    try:
        command(master_host, master_port, "CONFIG", "RESETSTAT")
    except RespError:
        pass

not_empty = []
for master_host, master_port in masters:
    dbsize = command(master_host, master_port, "DBSIZE")
    if int(dbsize) != 0:
        not_empty.append(f"{master_host}:{master_port} dbsize={dbsize}")

if not_empty:
    raise SystemExit("reset did not empty all masters: " + ", ".join(not_empty))

print("  flushed masters: " + ", ".join(f"{h}:{p}" for h, p in masters))
PY
}

warmup_cluster_data() {
  local cpu="$1"
  local payload="$2"
  local ratio="$3"
  local run_id="$4"
  local key_maximum="$5"
  local data_size_bytes="$6"
  local seed_file="$7"

  case "${WARMUP_BEFORE_RUNS}" in
    1|true|TRUE|yes|YES|on|ON)
      ;;
    *)
      return 0
      ;;
  esac

  echo "==> Warming target data before run ${run_id} (cpu=${cpu}, payload=${payload}KB, ratio=${ratio}, keys=${key_maximum})"
  memtier_benchmark \
    --server="${HOST}" --port="${PORT}" \
    --protocol=redis \
    --cluster-mode \
    "${TLS_ARGS[@]}" \
    --threads="${MEMTIER_WARMUP_THREADS}" --clients="${MEMTIER_WARMUP_CLIENTS}" \
    --pipeline="${MEMTIER_WARMUP_PIPELINE}" \
    --requests="${key_maximum}" \
    --key-maximum="${key_maximum}" \
    --key-pattern="${MEMTIER_WARMUP_KEY_PATTERN}" \
    --data-size="${data_size_bytes}" \
    "${RANDOM_DATA_ARGS[@]}" \
    --ratio="${MEMTIER_WARMUP_RATIO}" \
    --json-out-file "${seed_file}" \
    --run-count 1 \
    --print-percentiles="50,95,99,99.9"

  validate_single_run_result "${seed_file}"
}

validate_single_run_result() {
  local file="$1"

  python3 - "${file}" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as fh:
    doc = json.load(fh)

run = doc.get("ALL STATS") or doc.get("RUN #1 RESULTS")
if not isinstance(run, dict):
    raise SystemExit(f"{path}: missing ALL STATS/RUN #1 RESULTS")

runtime = run.get("Runtime") or {}
totals = run.get("Totals") or {}
finish = runtime.get("Finish time") or 0
ops = totals.get("Ops/sec") or 0
errors = totals.get("Connection Errors") or 0

if finish <= 0:
    raise SystemExit(f"{path}: invalid Finish time {finish}; memtier did not finalize cleanly")
if ops <= 0:
    raise SystemExit(f"{path}: invalid Ops/sec {ops}; check memtier logs for connection failures")
if errors:
    raise SystemExit(f"{path}: connection errors reported by memtier: {errors}")
PY
}

combine_run_results() {
  local output="$1"
  shift

  python3 - "${output}" "$@" <<'PY'
import json
import sys

output = sys.argv[1]
inputs = sys.argv[2:]
if not inputs:
    raise SystemExit("no run JSON files supplied")

combined = {}
for idx, path in enumerate(inputs, start=1):
    with open(path, "r", encoding="utf-8") as fh:
        doc = json.load(fh)

    run = doc.get("ALL STATS") or doc.get("RUN #1 RESULTS")
    if not isinstance(run, dict):
        raise SystemExit(f"{path}: missing ALL STATS/RUN #1 RESULTS")

    if idx == 1:
        combined["configuration"] = doc.get("configuration", {})
        combined["configuration"]["run_count"] = len(inputs)
        combined["configuration"]["print-all-runs"] = "true"
        combined["run information"] = doc.get("run information", {})

    combined[f"RUN #{idx} RESULTS"] = run

with open(output, "w", encoding="utf-8") as fh:
    json.dump(combined, fh, indent=2)
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
  local raw_dir="${OUTDIR}/raw/${tag}"
  local run_files=()
  local data_size_bytes=$((payload * 1024))
  local key_maximum

  key_maximum="$(effective_key_maximum "${payload}")"

  mkdir -p "${raw_dir}"
  echo "==> Case ${tag}: configured_keys=${KEYS}, effective_keys=${key_maximum}, target_dataset_mb=${TARGET_DATASET_MB}"

  for run_id in $(seq 1 "${N}"); do
    local run_file="${raw_dir}/run_${run_id}.json"
    local seed_file="${raw_dir}/seed_${run_id}.json"

    reset_cluster_data "${cpu}" "${payload}" "${ratio}" "${run_id}"
    warmup_cluster_data "${cpu}" "${payload}" "${ratio}" "${run_id}" "${key_maximum}" "${data_size_bytes}" "${seed_file}"

    echo "cpu=${cpu} payload=${payload}KB ratio=${ratio} run=${run_id}/${N}"
    memtier_benchmark \
      --server="${HOST}" --port="${PORT}" \
      --protocol=redis \
      --cluster-mode \
      "${TLS_ARGS[@]}" \
      --threads="${THREADS}" --clients="${CLIENTS}" \
      --pipeline="${PIPELINE}" \
      --test-time="${TEST_TIME}" \
      --key-maximum="${key_maximum}" \
      --data-size="${data_size_bytes}" \
      "${RANDOM_DATA_ARGS[@]}" \
      --ratio="${ratio}" \
      --json-out-file "${run_file}" \
      --run-count 1 \
      --print-percentiles="50,95,99,99.9" \
      "${STATSD_ARGS[@]}" \
      --statsd-run-label="${tag}_run_${run_id}"

    validate_single_run_result "${run_file}"
    run_files+=("${run_file}")
  done

  combine_run_results "${base}.json" "${run_files[@]}"

  annotate_result "${base}.json" "${cpu}" "${payload}" "${ratio}" "${tag}" "${key_maximum}"
}

echo "==> Benchmark configuration"
echo "PROVIDER=${PROVIDER}"
echo "VARIANT=${VARIANT}"
echo "BENCHMARKED_SYSTEM=${BENCHMARKED_SYSTEM}"
echo "HOST=${HOST}"
echo "PORT=${PORT}"
echo "NS=${NS}"
echo "RELEASE=${RELEASE}"
echo "STS=${STS}"
echo "CONTAINER=${CONTAINER}"
echo "N=${N}"
echo "MEMTIER_CPUS=${CPUs[*]}"
echo "MEMTIER_PAYLOADS=${PAYLOADS[*]}"
echo "MEMTIER_RATIOS=${RATIOS[*]}"
echo "MEMTIER_THREADS=${THREADS}"
echo "MEMTIER_CLIENTS=${CLIENTS}"
echo "MEMTIER_PIPELINE=${PIPELINE}"
echo "MEMTIER_TEST_TIME=${TEST_TIME}"
echo "MEMTIER_KEYS=${KEYS}"
echo "MEMTIER_TARGET_DATASET_MB=${TARGET_DATASET_MB}"
echo "MEMTIER_VALUE_OVERHEAD_BYTES=${VALUE_OVERHEAD_BYTES}"
echo "MEMTIER_RANDOM_DATA=${MEMTIER_RANDOM_DATA}"
echo "MEMTIER_RANDOM_DATA_ARGS=${RANDOM_DATA_ARGS[*]:-}"
echo "MEMTIER_TLS=${MEMTIER_TLS}"
echo "MEMTIER_TLS_SKIP_VERIFY=${MEMTIER_TLS_SKIP_VERIFY}"
echo "MEMTIER_TLS_ARGS=${TLS_ARGS[*]:-}"
echo "MEMTIER_STATSD_HOST=${STATSD_HOST}"
echo "MEMTIER_STATSD_PORT=${STATSD_PORT}"
echo "RESET_BETWEEN_RUNS=${RESET_BETWEEN_RUNS}"
echo "RESET_COMMAND_TIMEOUT=${RESET_COMMAND_TIMEOUT}"
echo "WARMUP_BEFORE_RUNS=${WARMUP_BEFORE_RUNS}"
echo "MEMTIER_WARMUP_THREADS=${MEMTIER_WARMUP_THREADS}"
echo "MEMTIER_WARMUP_CLIENTS=${MEMTIER_WARMUP_CLIENTS}"
echo "MEMTIER_WARMUP_PIPELINE=${MEMTIER_WARMUP_PIPELINE}"
echo "MEMTIER_WARMUP_RATIO=${MEMTIER_WARMUP_RATIO}"
echo "MEMTIER_WARMUP_KEY_PATTERN=${MEMTIER_WARMUP_KEY_PATTERN}"
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

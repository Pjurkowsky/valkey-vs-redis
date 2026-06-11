#!/usr/bin/env python3
"""Redis Cluster failover workload client.

This client is intentionally simpler than memtier for failover testing. It
keeps running when a node disappears, records client-visible successes and
failures per second, and writes a complete JSON report even when operations
fail during master promotion.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import ssl
import string
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from redis.cluster import RedisCluster


def parse_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    return value.lower() in {"1", "true", "yes", "y", "on"}


def parse_ratio(value: str) -> tuple[int, int]:
    parts = value.split(":")
    if len(parts) < 2:
        raise ValueError(f"ratio must be SET:GET, got {value!r}")
    set_weight = int(parts[0])
    get_weight = int(parts[1])
    if set_weight < 0 or get_weight < 0 or (set_weight + get_weight) <= 0:
        raise ValueError(f"ratio must contain positive SET or GET weight, got {value!r}")
    return set_weight, get_weight


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = pct / 100.0 * (len(ordered) - 1)
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def latency_summary(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {
            "samples": 0,
            "avg": None,
            "min": None,
            "max": None,
            "p50": None,
            "p95": None,
            "p99": None,
            "p999": None,
        }
    return {
        "samples": len(values),
        "avg": sum(values) / len(values),
        "min": min(values),
        "max": max(values),
        "p50": percentile(values, 50.0),
        "p95": percentile(values, 95.0),
        "p99": percentile(values, 99.0),
        "p999": percentile(values, 99.9),
    }


def new_bucket() -> dict[str, Any]:
    return {
        "attempted": 0,
        "succeeded": 0,
        "failed": 0,
        "set_attempted": 0,
        "set_succeeded": 0,
        "get_attempted": 0,
        "get_succeeded": 0,
        "latencies_ms": [],
        "error_types": Counter(),
    }


def merge_buckets(target: dict[str, Any], source: dict[str, Any], sample_limit: int) -> None:
    for key in (
        "attempted",
        "succeeded",
        "failed",
        "set_attempted",
        "set_succeeded",
        "get_attempted",
        "get_succeeded",
    ):
        target[key] += int(source.get(key, 0))

    target["error_types"].update(source.get("error_types", {}))
    remaining = max(0, sample_limit - len(target["latencies_ms"]))
    if remaining:
        target["latencies_ms"].extend(source.get("latencies_ms", [])[:remaining])


def make_client(args: argparse.Namespace) -> RedisCluster:
    kwargs: dict[str, Any] = {
        "host": args.host,
        "port": args.port,
        "decode_responses": True,
        "socket_timeout": args.socket_timeout,
        "socket_connect_timeout": args.connect_timeout,
        "retry_on_timeout": args.retry_on_timeout,
    }
    if args.tls:
        kwargs["ssl"] = True
        kwargs["ssl_cert_reqs"] = ssl.CERT_NONE if args.tls_skip_verify else ssl.CERT_REQUIRED
        if args.tls_ca_cert:
            kwargs["ssl_ca_certs"] = args.tls_ca_cert
        if args.tls_cert:
            kwargs["ssl_certfile"] = args.tls_cert
        if args.tls_key:
            kwargs["ssl_keyfile"] = args.tls_key
    return RedisCluster(**kwargs)


def close_client(client: RedisCluster | None) -> None:
    if client is None:
        return
    try:
        client.close()
    except Exception:
        pass


def record_latency(bucket: dict[str, Any], latency_ms: float, sample_limit: int) -> None:
    if len(bucket["latencies_ms"]) < sample_limit:
        bucket["latencies_ms"].append(latency_ms)


def worker(
    worker_id: int,
    args: argparse.Namespace,
    started_monotonic: float,
    payload: str,
    local_sample_limit: int,
) -> dict[str, Any]:
    rng = random.Random(args.seed + worker_id)
    set_weight, get_weight = parse_ratio(args.ratio)
    total_weight = set_weight + get_weight
    buckets: dict[int, dict[str, Any]] = defaultdict(new_bucket)
    total_errors: Counter[str] = Counter()
    error_samples: list[dict[str, Any]] = []
    client: RedisCluster | None = None

    def reconnect() -> RedisCluster:
        nonlocal client
        close_client(client)
        client = make_client(args)
        return client

    while True:
        now = time.monotonic()
        elapsed = now - started_monotonic
        if elapsed >= args.duration:
            break

        second = int(elapsed)
        bucket = buckets[second]
        key = f"{args.key_prefix}{rng.randrange(args.keys)}"
        op_is_set = rng.randrange(total_weight) < set_weight
        op_name = "set" if op_is_set else "get"

        bucket["attempted"] += 1
        bucket[f"{op_name}_attempted"] += 1
        op_started = time.monotonic()

        try:
            if client is None:
                client = make_client(args)
            if op_is_set:
                client.set(key, payload)
            else:
                client.get(key)
            latency_ms = (time.monotonic() - op_started) * 1000.0
            bucket["succeeded"] += 1
            bucket[f"{op_name}_succeeded"] += 1
            record_latency(bucket, latency_ms, local_sample_limit)
        except Exception as exc:
            latency_ms = (time.monotonic() - op_started) * 1000.0
            error_type = type(exc).__name__
            message = str(exc)
            bucket["failed"] += 1
            bucket["error_types"][error_type] += 1
            total_errors[error_type] += 1
            record_latency(bucket, latency_ms, local_sample_limit)
            if len(error_samples) < args.error_sample_limit:
                error_samples.append({
                    "second": second,
                    "worker": worker_id,
                    "operation": op_name,
                    "type": error_type,
                    "message": message,
                    "latency_ms": round(latency_ms, 3),
                })
            print(
                f"[s={second}] {op_name.upper()} failed worker={worker_id}: "
                f"{error_type}: {message}",
                flush=True,
            )
            close_client(client)
            client = None
            if args.reconnect_backoff_s > 0:
                time.sleep(args.reconnect_backoff_s)

    close_client(client)
    return {
        "buckets": buckets,
        "error_types": total_errors,
        "error_samples": error_samples,
    }


def build_report(
    args: argparse.Namespace,
    started_epoch_s: int,
    finished_epoch_s: int,
    worker_results: list[dict[str, Any]],
) -> dict[str, Any]:
    merged: dict[int, dict[str, Any]] = defaultdict(new_bucket)
    total_errors: Counter[str] = Counter()
    error_samples: list[dict[str, Any]] = []

    for result in worker_results:
        total_errors.update(result["error_types"])
        for sample in result["error_samples"]:
            if len(error_samples) < args.error_sample_limit:
                error_samples.append(sample)
        for second, bucket in result["buckets"].items():
            merge_buckets(merged[second], bucket, args.latency_sample_limit_per_second)

    per_second = []
    totals = new_bucket()
    all_latency_samples: list[float] = []
    for second in range(args.duration):
        bucket = merged[second]
        merge_buckets(totals, bucket, args.latency_sample_limit_total)
        remaining = max(0, args.latency_sample_limit_total - len(all_latency_samples))
        if remaining:
            all_latency_samples.extend(bucket["latencies_ms"][:remaining])

        per_second.append({
            "second": second,
            "attempted": bucket["attempted"],
            "succeeded": bucket["succeeded"],
            "failed": bucket["failed"],
            "attempted_ops_sec": bucket["attempted"],
            "success_ops_sec": bucket["succeeded"],
            "failed_ops_sec": bucket["failed"],
            "set_attempted": bucket["set_attempted"],
            "set_succeeded": bucket["set_succeeded"],
            "get_attempted": bucket["get_attempted"],
            "get_succeeded": bucket["get_succeeded"],
            "error_types": dict(sorted(bucket["error_types"].items())),
            "latency_ms": latency_summary(bucket["latencies_ms"]),
        })

    runtime_s = max(1, args.duration)
    total_attempted = totals["attempted"]
    total_succeeded = totals["succeeded"]
    total_failed = totals["failed"]

    return {
        "benchmark": "failover_client",
        "variant": "failover_client",
        "provider": args.provider,
        "system": args.system,
        "configuration": {
            "host": args.host,
            "port": args.port,
            "threads": args.threads,
            "clients": args.clients,
            "workers": args.threads * args.clients,
            "duration_s": args.duration,
            "ratio": args.ratio,
            "keys": args.keys,
            "data_size": args.data_size,
            "key_prefix": args.key_prefix,
            "socket_timeout_s": args.socket_timeout,
            "connect_timeout_s": args.connect_timeout,
            "retry_on_timeout": args.retry_on_timeout,
            "reconnect_backoff_s": args.reconnect_backoff_s,
            "tls": args.tls,
            "tls_skip_verify": args.tls_skip_verify,
            "seed": args.seed,
        },
        "run": {
            "started_epoch_s": started_epoch_s,
            "finished_epoch_s": finished_epoch_s,
            "duration_s": args.duration,
        },
        "totals": {
            "attempted": total_attempted,
            "succeeded": total_succeeded,
            "failed": total_failed,
            "attempted_ops_sec": total_attempted / runtime_s,
            "success_ops_sec": total_succeeded / runtime_s,
            "failed_ops_sec": total_failed / runtime_s,
            "success_rate": total_succeeded / total_attempted if total_attempted else None,
            "error_rate": total_failed / total_attempted if total_attempted else None,
            "set_attempted": totals["set_attempted"],
            "set_succeeded": totals["set_succeeded"],
            "get_attempted": totals["get_attempted"],
            "get_succeeded": totals["get_succeeded"],
            "error_types": dict(sorted(total_errors.items())),
            "latency_ms": latency_summary(all_latency_samples),
        },
        "error_samples": error_samples,
        "per_second": per_second,
    }


def make_payload(size: int) -> str:
    if size <= 0:
        return ""
    alphabet = string.ascii_letters + string.digits
    return "".join(alphabet[i % len(alphabet)] for i in range(size))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--duration", type=int, default=120)
    parser.add_argument("--output", required=True)
    parser.add_argument("--started-file", default="/tmp/failover.started")
    parser.add_argument("--provider", default="")
    parser.add_argument("--system", default="")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--clients", type=int, default=16)
    parser.add_argument("--ratio", default="1:1")
    parser.add_argument("--keys", type=int, default=100000)
    parser.add_argument("--data-size", type=int, default=1024)
    parser.add_argument("--key-prefix", default="failover-client:")
    parser.add_argument("--socket-timeout", type=float, default=1.0)
    parser.add_argument("--connect-timeout", type=float, default=1.0)
    parser.add_argument("--retry-on-timeout", action="store_true")
    parser.add_argument("--no-retry-on-timeout", dest="retry_on_timeout", action="store_false")
    parser.set_defaults(retry_on_timeout=False)
    parser.add_argument("--reconnect-backoff-s", type=float, default=0.01)
    parser.add_argument("--latency-sample-limit-per-second", type=int, default=5000)
    parser.add_argument("--latency-sample-limit-total", type=int, default=200000)
    parser.add_argument("--error-sample-limit", type=int, default=100)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--tls", action="store_true")
    parser.add_argument("--tls-skip-verify", action="store_true")
    parser.add_argument("--tls-ca-cert")
    parser.add_argument("--tls-cert")
    parser.add_argument("--tls-key")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.duration <= 0:
        raise SystemExit("--duration must be positive")
    if args.threads <= 0 or args.clients <= 0:
        raise SystemExit("--threads and --clients must be positive")
    if args.keys <= 0:
        raise SystemExit("--keys must be positive")

    workers = args.threads * args.clients
    local_sample_limit = max(1, args.latency_sample_limit_per_second // workers)
    payload = make_payload(args.data_size)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    print("==> failover_client configuration", flush=True)
    print(json.dumps({
        "host": args.host,
        "port": args.port,
        "duration_s": args.duration,
        "threads": args.threads,
        "clients": args.clients,
        "workers": workers,
        "ratio": args.ratio,
        "keys": args.keys,
        "data_size": args.data_size,
    }, sort_keys=True), flush=True)

    print("Connecting to cluster before starting workload...", flush=True)
    initial_client = make_client(args)
    initial_client.ping()
    close_client(initial_client)

    started_epoch_s = int(time.time())
    Path(args.started_file).write_text(f"{started_epoch_s}\n")
    started_monotonic = time.monotonic()
    print(f"Workload started at epoch {started_epoch_s}", flush=True)

    worker_results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(worker, worker_id, args, started_monotonic, payload, local_sample_limit)
            for worker_id in range(workers)
        ]
        for future in as_completed(futures):
            worker_results.append(future.result())

    finished_epoch_s = int(time.time())
    report = build_report(args, started_epoch_s, finished_epoch_s, worker_results)
    output.write_text(json.dumps(report, indent=2, sort_keys=True))

    totals = report["totals"]
    print("==> failover_client summary", flush=True)
    print(json.dumps({
        "attempted_ops_sec": totals["attempted_ops_sec"],
        "success_ops_sec": totals["success_ops_sec"],
        "failed_ops_sec": totals["failed_ops_sec"],
        "success_rate": totals["success_rate"],
        "error_types": totals["error_types"],
    }, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()

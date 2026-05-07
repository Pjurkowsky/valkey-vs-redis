#!/usr/bin/env python3
"""Consistency checker for Valkey cluster under network partition.

Writes keys continuously, tracks ACKs, then verifies all acknowledged
writes are still present. Outputs a JSON report.
"""

import argparse
import json
import os
import ssl
import time
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from pathlib import Path

from redis.cluster import RedisCluster
from redis.exceptions import (
    ClusterDownError,
    ConnectionError,
    RedisClusterException,
    TimeoutError,
)


def run_check(
    host: str,
    port: int,
    duration: int,
    run_id: str,
    output_path: Path,
    clients: int,
    socket_timeout: float,
    socket_connect_timeout: float,
    retry_on_timeout: bool,
    slow_threshold_ms: float,
    tls: bool,
    tls_skip_verify: bool,
    tls_ca_cert: str | None,
    tls_cert: str | None,
    tls_key: str | None,
) -> None:
    print(f"Connecting to {host}:{port} (cluster mode)...")
    redis_kwargs = {
        "host": host,
        "port": port,
        "decode_responses": True,
        "socket_timeout": socket_timeout,
        "socket_connect_timeout": socket_connect_timeout,
        "retry_on_timeout": retry_on_timeout,
    }
    if tls:
        redis_kwargs["ssl"] = True
        redis_kwargs["ssl_cert_reqs"] = ssl.CERT_NONE if tls_skip_verify else ssl.CERT_REQUIRED
        if tls_ca_cert:
            redis_kwargs["ssl_ca_certs"] = tls_ca_cert
        if tls_cert:
            redis_kwargs["ssl_certfile"] = tls_cert
        if tls_key:
            redis_kwargs["ssl_keyfile"] = tls_key

    rc = RedisCluster(
        **redis_kwargs,
    )
    rc.ping()
    print("Connected.")

    def make_client() -> RedisCluster:
        return RedisCluster(**redis_kwargs)

    def percentile(values: list[float], pct: float) -> float:
        if not values:
            return 0.0
        return float(np_percentile(values, pct))

    def worker(client_id: int) -> dict:
        rc_worker = make_client()
        local_seq = 0
        acked_keys: list[tuple[str, str]] = []
        failed_count = 0
        slow_count = 0
        affected_count = 0
        error_types: dict[str, int] = defaultdict(int)
        error_samples: list[dict] = []
        per_second: dict[int, dict] = defaultdict(
            lambda: {
                "attempted": 0,
                "acked": 0,
                "failed": 0,
                "slow": 0,
                "affected": 0,
                "latencies_ms": [],
            }
        )

        while True:
            elapsed = time.monotonic() - t_start
            if elapsed >= duration:
                break

            second = int(elapsed)
            key = f"dc:{run_id}:c{client_id}:{local_seq}"
            value = str(local_seq)

            op_start = time.monotonic()
            per_second[second]["attempted"] += 1
            try:
                rc_worker.set(key, value)
                latency_ms = (time.monotonic() - op_start) * 1000
                acked_keys.append((key, value))
                per_second[second]["acked"] += 1
                per_second[second]["latencies_ms"].append(latency_ms)
                if latency_ms >= slow_threshold_ms:
                    slow_count += 1
                    affected_count += 1
                    per_second[second]["slow"] += 1
                    per_second[second]["affected"] += 1
            except (ConnectionError, TimeoutError, ClusterDownError, RedisClusterException, OSError) as exc:
                latency_ms = (time.monotonic() - op_start) * 1000
                failed_count += 1
                affected_count += 1
                exc_name = type(exc).__name__
                error_types[exc_name] += 1
                if len(error_samples) < 20:
                    error_samples.append({
                        "second": second,
                        "client_id": client_id,
                        "seq": local_seq,
                        "type": exc_name,
                        "message": str(exc),
                        "latency_ms": round(latency_ms, 2),
                    })
                    print(
                        f"  [s={second}] Write failed client={client_id} seq={local_seq}: "
                        f"{exc_name}: {exc}",
                        flush=True,
                    )
                per_second[second]["failed"] += 1
                per_second[second]["affected"] += 1
                per_second[second]["latencies_ms"].append(latency_ms)

            local_seq += 1

        return {
            "acked_keys": acked_keys,
            "failed_count": failed_count,
            "slow_count": slow_count,
            "affected_count": affected_count,
            "error_types": dict(error_types),
            "error_samples": error_samples,
            "per_second": per_second,
        }

    acked_keys: list[tuple[str, str]] = []
    failed_error_types: dict[str, int] = defaultdict(int)
    failed_error_samples: list[dict] = []
    writes_per_second: dict[int, dict] = defaultdict(
        lambda: {
            "attempted": 0,
            "acked": 0,
            "failed": 0,
            "slow": 0,
            "affected": 0,
            "latencies_ms": [],
        }
    )
    t_start = time.monotonic()
    wall_start = time.time()

    print(
        f"Write phase: {duration}s (run_id={run_id}, clients={clients}, "
        f"timeout={socket_timeout}s, connect_timeout={socket_connect_timeout}s, "
        f"slow_threshold={slow_threshold_ms}ms)..."
    )
    with ThreadPoolExecutor(max_workers=clients) as pool:
        worker_results = list(pool.map(worker, range(clients)))

    total_failed = 0
    total_slow = 0
    total_affected = 0
    for result in worker_results:
        acked_keys.extend(result["acked_keys"])
        total_failed += result["failed_count"]
        total_slow += result["slow_count"]
        total_affected += result["affected_count"]
        for exc_name, count in result["error_types"].items():
            failed_error_types[exc_name] += count
        failed_error_samples.extend(result["error_samples"])
        failed_error_samples = failed_error_samples[:20]
        for second, entry in result["per_second"].items():
            aggregate = writes_per_second[second]
            aggregate["attempted"] += entry["attempted"]
            aggregate["acked"] += entry["acked"]
            aggregate["failed"] += entry["failed"]
            aggregate["slow"] += entry["slow"]
            aggregate["affected"] += entry["affected"]
            aggregate["latencies_ms"].extend(entry["latencies_ms"])

    write_duration = time.monotonic() - t_start
    total_attempted = sum(entry["attempted"] for entry in writes_per_second.values())
    print(
        f"Write phase done: {len(acked_keys)} ACK'd, {total_failed} failed, "
        f"{total_slow} slow, {total_affected} affected, {total_attempted} total "
        f"in {write_duration:.1f}s"
    )

    # -- Verify phase --
    print("Waiting 10s for cluster to stabilise before verification...")
    time.sleep(10)

    verify_kwargs = redis_kwargs.copy()
    verify_kwargs["socket_timeout"] = 10
    verify_kwargs["socket_connect_timeout"] = 10
    rc_verify = RedisCluster(**verify_kwargs)

    print(f"Verify phase: checking {len(acked_keys)} ACK'd keys...")
    missing_keys: list[str] = []
    verify_errors = 0

    batch_size = 500
    for batch_start in range(0, len(acked_keys), batch_size):
        batch = acked_keys[batch_start : batch_start + batch_size]
        for key, expected in batch:
            try:
                val = rc_verify.get(key)
                if val is None:
                    missing_keys.append(key)
                elif val != expected:
                    missing_keys.append(key)
            except (ConnectionError, TimeoutError, ClusterDownError, RedisClusterException, OSError):
                verify_errors += 1

        if (batch_start // batch_size) % 20 == 0 and batch_start > 0:
            print(f"  Verified {batch_start + len(batch)}/{len(acked_keys)}...")

    total_acked = len(acked_keys)
    keys_missing = len(missing_keys)
    loss_rate = keys_missing / total_acked if total_acked > 0 else 0.0
    affected_rate = total_affected / total_attempted if total_attempted > 0 else 0.0

    print(f"Verify done: {keys_missing} missing out of {total_acked} ACK'd "
          f"(loss rate: {loss_rate:.6f}), verify errors: {verify_errors}")

    # -- Build time series --
    ts_list = []
    for sec in sorted(writes_per_second.keys()):
        entry = writes_per_second[sec]
        latencies = entry["latencies_ms"]
        ts_list.append({
            "second": sec,
            "attempted": entry["attempted"],
            "acked": entry["acked"],
            "failed": entry["failed"],
            "slow": entry["slow"],
            "affected": entry["affected"],
            "p50_latency_ms": percentile(latencies, 50),
            "p95_latency_ms": percentile(latencies, 95),
            "p99_latency_ms": percentile(latencies, 99),
            "max_latency_ms": max(latencies) if latencies else 0.0,
        })

    all_latencies = [
        latency
        for entry in writes_per_second.values()
        for latency in entry["latencies_ms"]
    ]

    report = {
        "run_id": run_id,
        "host": host,
        "port": port,
        "clients": clients,
        "socket_timeout": socket_timeout,
        "socket_connect_timeout": socket_connect_timeout,
        "retry_on_timeout": retry_on_timeout,
        "slow_threshold_ms": slow_threshold_ms,
        "duration_requested": duration,
        "duration_actual": round(write_duration, 2),
        "wall_start": wall_start,
        "total_attempted": total_attempted,
        "total_acked": total_acked,
        "total_failed": total_failed,
        "total_slow": total_slow,
        "total_affected": total_affected,
        "affected_rate": affected_rate,
        "failed_rate": total_failed / total_attempted if total_attempted > 0 else 0.0,
        "slow_rate": total_slow / total_attempted if total_attempted > 0 else 0.0,
        "p50_latency_ms": percentile(all_latencies, 50),
        "p95_latency_ms": percentile(all_latencies, 95),
        "p99_latency_ms": percentile(all_latencies, 99),
        "max_latency_ms": max(all_latencies) if all_latencies else 0.0,
        "failed_error_types": dict(failed_error_types),
        "failed_error_samples": failed_error_samples,
        "keys_missing": keys_missing,
        "keys_lost_list": missing_keys,
        "loss_rate": loss_rate,
        "verify_errors": verify_errors,
        "write_rate_per_second": ts_list,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as f:
        json.dump(report, f, indent=2)

    print(f"Report written to {output_path}")

    # -- Cleanup test keys --
    print("Cleaning up test keys...")
    cleaned = 0
    for key, _ in acked_keys:
        try:
            rc_verify.delete(key)
            cleaned += 1
        except Exception:
            pass
    print(f"Cleaned {cleaned}/{total_acked} keys.")


def np_percentile(values: list[float], pct: float) -> float:
    values = sorted(values)
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]

    rank = (len(values) - 1) * pct / 100
    lower = int(rank)
    upper = min(lower + 1, len(values) - 1)
    weight = rank - lower
    return values[lower] * (1 - weight) + values[upper] * weight


def main() -> None:
    parser = argparse.ArgumentParser(description="Valkey consistency checker")
    parser.add_argument("--host", default="valkey.vk.svc.cluster.local")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--duration", type=int, default=120,
                        help="Write phase duration in seconds")
    parser.add_argument("--run-id", default=None,
                        help="Unique run identifier (default: timestamp)")
    parser.add_argument("--output", type=Path, default=Path("/work/results/consistency/consistency_result.json"),
                        help="Output JSON path")
    parser.add_argument("--clients", type=int, default=int(os.getenv("CONSISTENCY_CLIENTS", "50")),
                        help="Number of concurrent simulated clients")
    parser.add_argument("--socket-timeout", type=float,
                        default=float(os.getenv("CONSISTENCY_SOCKET_TIMEOUT", "1.0")),
                        help="Per-request socket timeout in seconds")
    parser.add_argument("--connect-timeout", type=float,
                        default=float(os.getenv("CONSISTENCY_CONNECT_TIMEOUT", "1.0")),
                        help="Connection timeout in seconds")
    parser.add_argument("--retry-on-timeout", action=argparse.BooleanOptionalAction,
                        default=os.getenv("CONSISTENCY_RETRY_ON_TIMEOUT", "false").lower() in {"1", "true", "yes", "on"},
                        help="Whether redis-py should retry timed-out operations")
    parser.add_argument("--slow-threshold-ms", type=float,
                        default=float(os.getenv("CONSISTENCY_SLOW_THRESHOLD_MS", "1000")),
                        help="Requests at or above this latency are counted as user-visible problems")
    parser.add_argument("--tls", action="store_true",
                        help="Connect to Valkey using TLS")
    parser.add_argument("--tls-skip-verify", action="store_true",
                        help="Disable TLS certificate verification")
    parser.add_argument("--tls-ca-cert", default=None,
                        help="Path to TLS CA certificate")
    parser.add_argument("--tls-cert", default=None,
                        help="Path to TLS client certificate")
    parser.add_argument("--tls-key", default=None,
                        help="Path to TLS client private key")
    args = parser.parse_args()

    if args.run_id is None:
        args.run_id = str(int(time.time()))

    run_check(
        host=args.host,
        port=args.port,
        duration=args.duration,
        run_id=args.run_id,
        output_path=args.output,
        clients=args.clients,
        socket_timeout=args.socket_timeout,
        socket_connect_timeout=args.connect_timeout,
        retry_on_timeout=args.retry_on_timeout,
        slow_threshold_ms=args.slow_threshold_ms,
        tls=args.tls,
        tls_skip_verify=args.tls_skip_verify,
        tls_ca_cert=args.tls_ca_cert,
        tls_cert=args.tls_cert,
        tls_key=args.tls_key,
    )


if __name__ == "__main__":
    main()

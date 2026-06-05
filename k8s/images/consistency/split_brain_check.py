#!/usr/bin/env python3
"""Split-brain consistency checker for Valkey cluster.

Writes keys that are distributed across all hash slots (no hash tags),
tracks ACKs, then verifies all acknowledged writes after partition heals.
Keys written to minority-side slots may be lost when the majority promotes
a new master and the old minority master rejoins as a replica.

Outputs a JSON report with per-side breakdown of data loss.
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


def crc16(data: bytes) -> int:
    """CRC16/CCITT used by Redis Cluster for slot hashing."""
    crc = 0
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = (crc << 1) ^ 0x1021
            else:
                crc <<= 1
            crc &= 0xFFFF
    return crc


def key_slot(key: str) -> int:
    return crc16(key.encode()) % 16384


def parse_slot_spec(slot_spec: str | None) -> set[int]:
    """Parse comma-separated Redis Cluster slot numbers/ranges."""
    slots: set[int] = set()
    if not slot_spec:
        return slots

    for raw_part in slot_spec.split(","):
        part = raw_part.strip()
        if not part:
            continue
        if "-" in part:
            start_raw, end_raw = part.split("-", 1)
            start, end = int(start_raw), int(end_raw)
        else:
            start = end = int(part)
        if start < 0 or end > 16383 or start > end:
            raise ValueError(f"Invalid Redis Cluster slot range: {part}")
        slots.update(range(start, end + 1))
    return slots


def discover_slot_owners(rc: RedisCluster) -> dict[int, str]:
    """Map each slot to the node address that owns it."""
    slot_map: dict[int, str] = {}
    slots_info = rc.execute_command("CLUSTER", "SLOTS")
    for entry in slots_info:
        start_slot, end_slot = int(entry[0]), int(entry[1])
        master_host = entry[2][0]
        if isinstance(master_host, bytes):
            master_host = master_host.decode()
        master_port = int(entry[2][1])
        addr = f"{master_host}:{master_port}"
        for s in range(start_slot, end_slot + 1):
            slot_map[s] = addr
    return slot_map


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
    minority_pods: list[str],
    tls: bool,
    tls_skip_verify: bool,
    tls_ca_cert: str | None,
    tls_cert: str | None,
    tls_key: str | None,
    minority_slots_spec: str | None,
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

    rc = RedisCluster(**redis_kwargs)
    rc.ping()
    print("Connected.")

    slot_owners = discover_slot_owners(rc)
    minority_slots = parse_slot_spec(minority_slots_spec)
    minority_addrs: set[str] = set()
    if not minority_slots:
        minority_set = set(minority_pods)
        for addr in slot_owners.values():
            node_host = addr.split(":")[0]
            for mp in minority_set:
                if mp in node_host or node_host in mp:
                    minority_addrs.add(addr)
        minority_slots = {s for s, a in slot_owners.items() if a in minority_addrs}

    if not minority_slots:
        raise RuntimeError(
            "No minority slots were identified. Pass --minority-slots or ensure "
            "CLUSTER SLOTS announces hosts that match --minority-pods."
        )

    majority_slots = set(range(16384)) - minority_slots

    print(f"Minority pods: {minority_pods}")
    print(f"Minority slot spec: {minority_slots_spec or 'inferred-from-pods'}")
    print(f"Minority addresses: {minority_addrs}")
    print(f"Minority slot count: {len(minority_slots)}")
    print(f"Majority slot count: {len(majority_slots)}")

    def make_client() -> RedisCluster:
        return RedisCluster(**redis_kwargs)

    def percentile(values: list[float], pct: float) -> float:
        if not values:
            return 0.0
        return float(np_percentile(values, pct))

    def worker(client_id: int) -> dict:
        rc_worker = make_client()
        local_seq = 0
        acked_keys: list[tuple[str, str, str]] = []
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
                "acked_minority": 0,
                "acked_majority": 0,
                "failed_minority": 0,
                "failed_majority": 0,
                "latencies_ms": [],
            }
        )

        while True:
            elapsed = time.monotonic() - t_start
            if elapsed >= duration:
                break

            second = int(elapsed)
            key = f"sb.{run_id}.c{client_id}.{local_seq}"
            value = str(local_seq)
            slot = key_slot(key)
            side = "minority" if slot in minority_slots else "majority"

            op_start = time.monotonic()
            per_second[second]["attempted"] += 1
            try:
                rc_worker.set(key, value)
                latency_ms = (time.monotonic() - op_start) * 1000
                acked_keys.append((key, value, side))
                per_second[second]["acked"] += 1
                per_second[second][f"acked_{side}"] += 1
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
                per_second[second]["failed"] += 1
                per_second[second][f"failed_{side}"] += 1
                per_second[second]["affected"] += 1
                if len(error_samples) < 20:
                    error_samples.append({
                        "second": second,
                        "client_id": client_id,
                        "seq": local_seq,
                        "side": side,
                        "type": exc_name,
                        "message": str(exc),
                        "latency_ms": round(latency_ms, 2),
                    })
                    print(
                        f"  [s={second}] Write failed client={client_id} seq={local_seq} "
                        f"side={side}: {exc_name}: {exc}",
                        flush=True,
                    )
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

    acked_keys: list[tuple[str, str, str]] = []
    failed_error_types: dict[str, int] = defaultdict(int)
    failed_error_samples: list[dict] = []
    writes_per_second: dict[int, dict] = defaultdict(
        lambda: {
            "attempted": 0,
            "acked": 0,
            "failed": 0,
            "slow": 0,
            "affected": 0,
            "acked_minority": 0,
            "acked_majority": 0,
            "failed_minority": 0,
            "failed_majority": 0,
            "latencies_ms": [],
        }
    )
    t_start = time.monotonic()
    wall_start = time.time()

    print(
        f"Write phase: {duration}s (run_id={run_id}, clients={clients}, "
        f"timeout={socket_timeout}s, slow_threshold={slow_threshold_ms}ms)..."
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
            for field in aggregate:
                if field == "latencies_ms":
                    aggregate[field].extend(entry[field])
                else:
                    aggregate[field] += entry[field]

    write_duration = time.monotonic() - t_start
    total_attempted = sum(e["attempted"] for e in writes_per_second.values())

    acked_minority = sum(1 for _, _, s in acked_keys if s == "minority")
    acked_majority = sum(1 for _, _, s in acked_keys if s == "majority")

    print(
        f"Write phase done: {len(acked_keys)} ACK'd "
        f"(minority={acked_minority}, majority={acked_majority}), "
        f"{total_failed} failed, {total_affected} affected "
        f"in {write_duration:.1f}s"
    )

    # -- Verify phase --
    print("Waiting 15s for cluster to stabilise before verification...")
    time.sleep(15)

    verify_kwargs = redis_kwargs.copy()
    verify_kwargs["socket_timeout"] = 10
    verify_kwargs["socket_connect_timeout"] = 10
    rc_verify = RedisCluster(**verify_kwargs)

    print(f"Verify phase: checking {len(acked_keys)} ACK'd keys...")
    missing_minority: list[str] = []
    missing_majority: list[str] = []
    verify_errors = 0

    batch_size = 500
    for batch_start in range(0, len(acked_keys), batch_size):
        batch = acked_keys[batch_start : batch_start + batch_size]
        for key, expected, side in batch:
            try:
                val = rc_verify.get(key)
                if val is None or val != expected:
                    if side == "minority":
                        missing_minority.append(key)
                    else:
                        missing_majority.append(key)
            except (ConnectionError, TimeoutError, ClusterDownError, RedisClusterException, OSError):
                verify_errors += 1

        if (batch_start // batch_size) % 20 == 0 and batch_start > 0:
            print(f"  Verified {batch_start + len(batch)}/{len(acked_keys)}...")

    total_acked = len(acked_keys)
    keys_missing_minority = len(missing_minority)
    keys_missing_majority = len(missing_majority)
    keys_missing_total = keys_missing_minority + keys_missing_majority
    loss_rate = keys_missing_total / total_acked if total_acked > 0 else 0.0
    minority_loss_rate = keys_missing_minority / acked_minority if acked_minority > 0 else 0.0
    majority_loss_rate = keys_missing_majority / acked_majority if acked_majority > 0 else 0.0

    print(
        f"Verify done: {keys_missing_total} missing "
        f"(minority={keys_missing_minority}/{acked_minority}, "
        f"majority={keys_missing_majority}/{acked_majority}), "
        f"verify errors={verify_errors}"
    )

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
            "acked_minority": entry["acked_minority"],
            "acked_majority": entry["acked_majority"],
            "failed_minority": entry["failed_minority"],
            "failed_majority": entry["failed_majority"],
            "p50_latency_ms": percentile(latencies, 50),
            "p95_latency_ms": percentile(latencies, 95),
            "p99_latency_ms": percentile(latencies, 99),
            "max_latency_ms": max(latencies) if latencies else 0.0,
        })

    all_latencies = [
        lat for e in writes_per_second.values() for lat in e["latencies_ms"]
    ]

    report = {
        "run_id": run_id,
        "test_type": "split_brain",
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
        "minority_pods": minority_pods,
        "minority_slots_spec": minority_slots_spec,
        "minority_slot_count": len(minority_slots),
        "majority_slot_count": len(majority_slots),
        "total_attempted": total_attempted,
        "total_acked": total_acked,
        "acked_minority": acked_minority,
        "acked_majority": acked_majority,
        "total_failed": total_failed,
        "total_slow": total_slow,
        "total_affected": total_affected,
        "affected_rate": total_affected / total_attempted if total_attempted > 0 else 0.0,
        "failed_rate": total_failed / total_attempted if total_attempted > 0 else 0.0,
        "p50_latency_ms": percentile(all_latencies, 50),
        "p95_latency_ms": percentile(all_latencies, 95),
        "p99_latency_ms": percentile(all_latencies, 99),
        "max_latency_ms": max(all_latencies) if all_latencies else 0.0,
        "failed_error_types": dict(failed_error_types),
        "failed_error_samples": failed_error_samples,
        "keys_missing": keys_missing_total,
        "keys_missing_minority": keys_missing_minority,
        "keys_missing_majority": keys_missing_majority,
        "minority_loss_rate": minority_loss_rate,
        "majority_loss_rate": majority_loss_rate,
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
    for key, _, _ in acked_keys:
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
    parser = argparse.ArgumentParser(description="Valkey split-brain consistency checker")
    parser.add_argument("--host", default="valkey.vk.svc.cluster.local")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--duration", type=int, default=120,
                        help="Write phase duration in seconds")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--output", type=Path,
                        default=Path("/work/results/split_brain/split_brain_result.json"))
    parser.add_argument("--clients", type=int,
                        default=int(os.getenv("SPLIT_BRAIN_CLIENTS", "50")))
    parser.add_argument("--socket-timeout", type=float,
                        default=float(os.getenv("SPLIT_BRAIN_SOCKET_TIMEOUT", "1.0")))
    parser.add_argument("--connect-timeout", type=float,
                        default=float(os.getenv("SPLIT_BRAIN_CONNECT_TIMEOUT", "1.0")))
    parser.add_argument("--retry-on-timeout", action=argparse.BooleanOptionalAction,
                        default=os.getenv("SPLIT_BRAIN_RETRY_ON_TIMEOUT", "false").lower()
                        in {"1", "true", "yes", "on"})
    parser.add_argument("--slow-threshold-ms", type=float,
                        default=float(os.getenv("SPLIT_BRAIN_SLOW_THRESHOLD_MS", "1000")))
    parser.add_argument("--minority-pods", required=True,
                        help="Comma-separated list of minority pod names (e.g. valkey-2,valkey-5)")
    parser.add_argument("--minority-slots", default=None,
                        help="Comma-separated minority slot ranges (e.g. 0-5460,12000)")
    parser.add_argument("--tls", action="store_true")
    parser.add_argument("--tls-skip-verify", action="store_true")
    parser.add_argument("--tls-ca-cert", default=None)
    parser.add_argument("--tls-cert", default=None)
    parser.add_argument("--tls-key", default=None)
    args = parser.parse_args()

    if args.run_id is None:
        args.run_id = str(int(time.time()))

    minority_pods = [p.strip() for p in args.minority_pods.split(",") if p.strip()]

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
        minority_pods=minority_pods,
        tls=args.tls,
        tls_skip_verify=args.tls_skip_verify,
        tls_ca_cert=args.tls_ca_cert,
        tls_cert=args.tls_cert,
        tls_key=args.tls_key,
        minority_slots_spec=args.minority_slots,
    )


if __name__ == "__main__":
    main()

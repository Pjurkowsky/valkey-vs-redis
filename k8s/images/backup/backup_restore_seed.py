#!/usr/bin/env python3
"""Seed data into a Valkey cluster and verify restoration after restart.

Modes:
  seed   -- bulk-write keys until the target size is reached
  verify -- read back a random sample of seeded keys and check integrity
"""

import argparse
import json
import random
import time
from pathlib import Path

from redis import Redis
from redis.cluster import RedisCluster
from redis.exceptions import (
    ClusterDownError,
    ConnectionError,
    RedisError,
    RedisClusterException,
    TimeoutError,
)

VALUE_SIZE = 1024  # 1 KB per key
PIPELINE_BATCH = 500


def _connect(host: str, port: int) -> RedisCluster:
    return RedisCluster(
        host=host,
        port=port,
        decode_responses=True,
        socket_timeout=10,
        socket_connect_timeout=10,
        retry_on_timeout=True,
    )


def _connect_single(host: str, port: int) -> Redis:
    return Redis(
        host=host,
        port=port,
        decode_responses=True,
        socket_timeout=10,
        socket_connect_timeout=10,
        retry_on_timeout=True,
    )


def _is_oom(exc: BaseException) -> bool:
    text = str(exc).lower()
    return "oom" in text or "used memory > 'maxmemory'" in text


def _master_addrs(host: str, port: int) -> list[tuple[str, int]]:
    client = _connect_single(host, port)
    raw_nodes = client.execute_command("CLUSTER", "NODES")
    if isinstance(raw_nodes, bytes):
        raw_nodes = raw_nodes.decode()

    masters = []
    for line in str(raw_nodes).splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        flags = set(parts[2].split(","))
        if "master" not in flags or "fail" in flags or "handshake" in flags:
            continue
        endpoint = parts[1].split("@", 1)[0].split(",", 1)[0]
        node_host, node_port = endpoint.rsplit(":", 1)
        masters.append((node_host, int(node_port)))
    return masters


def seed(
    host: str,
    port: int,
    target_mb: int,
    run_id: str,
    output: Path,
    ttl_seconds: int | None = None,
    allow_partial: bool = False,
    stop_after_errors: int = 1000,
) -> None:
    rc = _connect(host, port)
    rc.ping()

    target_bytes = target_mb * 1024 * 1024
    target_keys = max(1, target_bytes // VALUE_SIZE)
    value = "x" * VALUE_SIZE

    print(f"Seeding {target_keys} keys ({target_mb} MB, {VALUE_SIZE}B each), run_id={run_id}...")
    t_start = time.monotonic()
    written = 0
    errors = 0
    write_errors = 0
    oom_errors = 0
    first_error = None
    last_error = None
    stopped_early = False

    for batch_start in range(0, target_keys, PIPELINE_BATCH):
        batch_end = min(batch_start + PIPELINE_BATCH, target_keys)
        pipe = rc.pipeline(transaction=False)
        for i in range(batch_start, batch_end):
            pipe.set(f"br:{run_id}:{i}", value, ex=ttl_seconds)
        try:
            pipe.execute()
            written += batch_end - batch_start
        except (ConnectionError, TimeoutError, ClusterDownError, RedisClusterException, RedisError, OSError) as exc:
            errors += 1
            last_error = f"{type(exc).__name__}: {exc}"
            first_error = first_error or last_error
            if _is_oom(exc):
                oom_errors += 1
            if errors <= 5:
                print(f"  Pipeline error at {batch_start}: {type(exc).__name__}: {exc}")
            for i in range(batch_start, batch_end):
                try:
                    rc.set(f"br:{run_id}:{i}", value, ex=ttl_seconds)
                    written += 1
                except Exception as item_exc:
                    write_errors += 1
                    last_error = f"{type(item_exc).__name__}: {item_exc}"
                    first_error = first_error or last_error
                    if _is_oom(item_exc):
                        oom_errors += 1
            if stop_after_errors > 0 and write_errors >= stop_after_errors:
                stopped_early = True
                print(f"  Stopping early after {write_errors} write errors.")
                break

        if batch_start % (PIPELINE_BATCH * 100) == 0 and batch_start > 0:
            pct = batch_start / target_keys * 100
            print(f"  {pct:.0f}% ({batch_start}/{target_keys})")

    duration = time.monotonic() - t_start
    completed = written == target_keys
    print(
        f"Seed complete: {written}/{target_keys} keys in {duration:.1f}s "
        f"({errors} pipeline errors, {write_errors} write errors, {oom_errors} OOM errors)"
    )

    report = {
        "mode": "seed",
        "run_id": run_id,
        "target_mb": target_mb,
        "target_keys": target_keys,
        "written_keys": written,
        "total_bytes": written * VALUE_SIZE,
        "seed_duration_s": round(duration, 2),
        "errors": errors,
        "write_errors": write_errors,
        "oom_errors": oom_errors,
        "completed": completed,
        "stopped_early": stopped_early,
        "ttl_seconds": ttl_seconds,
        "allow_partial": allow_partial,
        "first_error": first_error,
        "last_error": last_error,
    }

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w") as f:
        json.dump(report, f, indent=2)
    print(f"Seed report: {output}")

    if not allow_partial and (oom_errors > 0 or stopped_early):
        raise SystemExit(2)


def verify(host: str, port: int, seed_report_path: Path, output: Path) -> None:
    with seed_report_path.open() as f:
        seed_report = json.load(f)

    run_id = seed_report["run_id"]
    total_keys = seed_report["written_keys"]
    sample_ratio = 0.10
    sample_size = max(100, int(total_keys * sample_ratio))
    sample_size = min(sample_size, total_keys)

    indices = sorted(random.sample(range(total_keys), sample_size))

    rc = _connect(host, port)
    rc.ping()

    print(f"Verifying {sample_size} keys (10% sample of {total_keys})...")
    t_start = time.monotonic()
    found = 0
    missing = 0
    verify_errors = 0

    for idx in indices:
        key = f"br:{run_id}:{idx}"
        try:
            val = rc.get(key)
            if val is not None and len(val) == VALUE_SIZE:
                found += 1
            else:
                missing += 1
        except (ConnectionError, TimeoutError, ClusterDownError, RedisClusterException, OSError):
            verify_errors += 1

    duration = time.monotonic() - t_start
    integrity_ok = missing == 0 and verify_errors == 0

    print(f"Verify complete: {found} found, {missing} missing, "
          f"{verify_errors} errors in {duration:.1f}s — {'OK' if integrity_ok else 'FAILED'}")

    report = {
        "mode": "verify",
        "run_id": run_id,
        "total_keys": total_keys,
        "sample_size": sample_size,
        "keys_found": found,
        "keys_missing": missing,
        "verify_errors": verify_errors,
        "integrity_ok": integrity_ok,
        "verify_duration_s": round(duration, 2),
    }

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w") as f:
        json.dump(report, f, indent=2)
    print(f"Verify report: {output}")


def cleanup(host: str, port: int, seed_report_path: Path) -> None:
    with seed_report_path.open() as f:
        seed_report = json.load(f)

    run_id = seed_report["run_id"]
    total_keys = seed_report["written_keys"]

    rc = _connect(host, port)
    print(f"Cleaning up {total_keys} keys...")

    for batch_start in range(0, total_keys, PIPELINE_BATCH):
        batch_end = min(batch_start + PIPELINE_BATCH, total_keys)
        pipe = rc.pipeline(transaction=False)
        for i in range(batch_start, batch_end):
            pipe.delete(f"br:{run_id}:{i}")
        try:
            pipe.execute()
        except Exception:
            pass

    print("Cleanup done.")


def snapshot(host: str, port: int, phase: str, output: Path) -> None:
    masters = []
    for node_host, node_port in _master_addrs(host, port):
        client = _connect_single(node_host, node_port)
        info = client.info("all")
        config = client.config_get("maxmemory-policy")
        masters.append({
            "addr": f"{node_host}:{node_port}",
            "used_memory": int(info.get("used_memory", 0)),
            "used_memory_rss": int(info.get("used_memory_rss", 0)),
            "maxmemory": int(info.get("maxmemory", 0)),
            "maxmemory_policy": config.get("maxmemory-policy", ""),
            "evicted_keys": int(info.get("evicted_keys", 0)),
            "expired_keys": int(info.get("expired_keys", 0)),
            "keyspace_hits": int(info.get("keyspace_hits", 0)),
            "keyspace_misses": int(info.get("keyspace_misses", 0)),
            "total_error_replies": int(info.get("total_error_replies", 0)),
            "dbsize": int(client.dbsize()),
        })

    report = {
        "mode": "snapshot",
        "phase": phase,
        "timestamp_s": int(time.time()),
        "masters": masters,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w") as f:
        json.dump(report, f, indent=2)
    print(f"Snapshot report: {output}")


def flush_cluster(host: str, port: int, reset_stats: bool) -> None:
    for node_host, node_port in _master_addrs(host, port):
        client = _connect_single(node_host, node_port)
        client.flushall()
        if reset_stats:
            try:
                client.config_resetstat()
            except RedisError as exc:
                print(f"Could not reset stats on {node_host}:{node_port}: {exc}")
    print("Flush complete.")


def configure_cluster(host: str, port: int, maxmemory_policy: str | None, maxmemory: str | None) -> None:
    for node_host, node_port in _master_addrs(host, port):
        client = _connect_single(node_host, node_port)
        if maxmemory_policy:
            client.config_set("maxmemory-policy", maxmemory_policy)
        if maxmemory:
            client.config_set("maxmemory", maxmemory)
    print("Configuration applied.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backup/restore seed & verify tool")
    parser.add_argument("--mode", required=True, choices=["seed", "verify", "cleanup", "snapshot", "flush", "configure"])
    parser.add_argument("--host", default="valkey.vk.svc.cluster.local")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--target-mb", type=int, default=100,
                        help="Target dataset size in MB per shard (seed mode)")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--ttl-seconds", type=int, default=0,
                        help="TTL for seeded keys; 0 means no TTL")
    parser.add_argument("--allow-partial", action="store_true",
                        help="Exit successfully even if maxmemory prevents writing all keys")
    parser.add_argument("--stop-after-errors", type=int, default=1000,
                        help="Stop seed mode after this many per-key write errors; 0 disables")
    parser.add_argument("--phase", default="snapshot")
    parser.add_argument("--reset-stats", action="store_true")
    parser.add_argument("--maxmemory-policy", default=None)
    parser.add_argument("--maxmemory", default=None)
    parser.add_argument("--seed-report", type=Path, default=None,
                        help="Path to seed report JSON (verify/cleanup mode)")
    parser.add_argument("--output", type=Path,
                        default=Path("/work/results/backup/report.json"))
    args = parser.parse_args()

    if args.run_id is None:
        args.run_id = str(int(time.time()))

    if args.mode == "seed":
        ttl_seconds = args.ttl_seconds if args.ttl_seconds > 0 else None
        seed(
            args.host,
            args.port,
            args.target_mb,
            args.run_id,
            args.output,
            ttl_seconds=ttl_seconds,
            allow_partial=args.allow_partial,
            stop_after_errors=args.stop_after_errors,
        )
    elif args.mode == "verify":
        if args.seed_report is None:
            parser.error("--seed-report required for verify mode")
        verify(args.host, args.port, args.seed_report, args.output)
    elif args.mode == "cleanup":
        if args.seed_report is None:
            parser.error("--seed-report required for cleanup mode")
        cleanup(args.host, args.port, args.seed_report)
    elif args.mode == "snapshot":
        snapshot(args.host, args.port, args.phase, args.output)
    elif args.mode == "flush":
        flush_cluster(args.host, args.port, args.reset_stats)
    elif args.mode == "configure":
        configure_cluster(args.host, args.port, args.maxmemory_policy, args.maxmemory)


if __name__ == "__main__":
    main()

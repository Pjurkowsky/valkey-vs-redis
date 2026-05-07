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

from redis.cluster import RedisCluster
from redis.exceptions import (
    ClusterDownError,
    ConnectionError,
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


def seed(host: str, port: int, target_mb: int, run_id: str, output: Path) -> None:
    rc = _connect(host, port)
    rc.ping()

    target_bytes = target_mb * 1024 * 1024
    target_keys = max(1, target_bytes // VALUE_SIZE)
    value = "x" * VALUE_SIZE

    print(f"Seeding {target_keys} keys ({target_mb} MB, {VALUE_SIZE}B each), run_id={run_id}...")
    t_start = time.monotonic()
    written = 0
    errors = 0

    for batch_start in range(0, target_keys, PIPELINE_BATCH):
        batch_end = min(batch_start + PIPELINE_BATCH, target_keys)
        pipe = rc.pipeline(transaction=False)
        for i in range(batch_start, batch_end):
            pipe.set(f"br:{run_id}:{i}", value)
        try:
            pipe.execute()
            written += batch_end - batch_start
        except (ConnectionError, TimeoutError, ClusterDownError, RedisClusterException, OSError) as exc:
            errors += 1
            if errors <= 5:
                print(f"  Pipeline error at {batch_start}: {type(exc).__name__}")
            for i in range(batch_start, batch_end):
                try:
                    rc.set(f"br:{run_id}:{i}", value)
                    written += 1
                except Exception:
                    pass

        if batch_start % (PIPELINE_BATCH * 100) == 0 and batch_start > 0:
            pct = batch_start / target_keys * 100
            print(f"  {pct:.0f}% ({batch_start}/{target_keys})")

    duration = time.monotonic() - t_start
    print(f"Seed complete: {written} keys in {duration:.1f}s ({errors} pipeline errors)")

    report = {
        "mode": "seed",
        "run_id": run_id,
        "target_mb": target_mb,
        "target_keys": target_keys,
        "written_keys": written,
        "total_bytes": written * VALUE_SIZE,
        "seed_duration_s": round(duration, 2),
        "errors": errors,
    }

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w") as f:
        json.dump(report, f, indent=2)
    print(f"Seed report: {output}")


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Backup/restore seed & verify tool")
    parser.add_argument("--mode", required=True, choices=["seed", "verify", "cleanup"])
    parser.add_argument("--host", default="valkey.vk.svc.cluster.local")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--target-mb", type=int, default=100,
                        help="Target dataset size in MB per shard (seed mode)")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--seed-report", type=Path, default=None,
                        help="Path to seed report JSON (verify/cleanup mode)")
    parser.add_argument("--output", type=Path,
                        default=Path("/work/results/backup/report.json"))
    args = parser.parse_args()

    if args.run_id is None:
        args.run_id = str(int(time.time()))

    if args.mode == "seed":
        seed(args.host, args.port, args.target_mb, args.run_id, args.output)
    elif args.mode == "verify":
        if args.seed_report is None:
            parser.error("--seed-report required for verify mode")
        verify(args.host, args.port, args.seed_report, args.output)
    elif args.mode == "cleanup":
        if args.seed_report is None:
            parser.error("--seed-report required for cleanup mode")
        cleanup(args.host, args.port, args.seed_report)


if __name__ == "__main__":
    main()

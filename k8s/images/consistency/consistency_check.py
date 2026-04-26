#!/usr/bin/env python3
"""Consistency checker for Valkey cluster under network partition.

Writes keys continuously, tracks ACKs, then verifies all acknowledged
writes are still present. Outputs a JSON report.
"""

import argparse
import json
import os
import time
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
) -> None:
    print(f"Connecting to {host}:{port} (cluster mode)...")
    rc = RedisCluster(
        host=host,
        port=port,
        decode_responses=True,
        socket_timeout=5,
        socket_connect_timeout=5,
        retry_on_timeout=True,
    )
    rc.ping()
    print("Connected.")

    acked_seqs: list[int] = []
    failed_seqs: list[int] = []
    writes_per_second: dict[int, dict] = defaultdict(
        lambda: {"attempted": 0, "acked": 0, "failed": 0}
    )

    seq = 0
    t_start = time.monotonic()
    wall_start = time.time()

    print(f"Write phase: {duration}s (run_id={run_id})...")
    while True:
        elapsed = time.monotonic() - t_start
        if elapsed >= duration:
            break

        second = int(elapsed)
        key = f"dc:{run_id}:{seq}"
        value = str(seq)

        writes_per_second[second]["attempted"] += 1
        try:
            rc.set(key, value)
            acked_seqs.append(seq)
            writes_per_second[second]["acked"] += 1
        except (ConnectionError, TimeoutError, ClusterDownError, RedisClusterException, OSError) as exc:
            failed_seqs.append(seq)
            writes_per_second[second]["failed"] += 1
            if seq % 500 == 0:
                print(f"  [s={second}] Write failed seq={seq}: {type(exc).__name__}")

        seq += 1

    write_duration = time.monotonic() - t_start
    print(f"Write phase done: {len(acked_seqs)} ACK'd, {len(failed_seqs)} failed, "
          f"{seq} total in {write_duration:.1f}s")

    # -- Verify phase --
    print("Waiting 10s for cluster to stabilise before verification...")
    time.sleep(10)

    rc_verify = RedisCluster(
        host=host,
        port=port,
        decode_responses=True,
        socket_timeout=10,
        socket_connect_timeout=10,
        retry_on_timeout=True,
    )

    print(f"Verify phase: checking {len(acked_seqs)} ACK'd keys...")
    missing_keys: list[str] = []
    verify_errors = 0

    batch_size = 500
    for batch_start in range(0, len(acked_seqs), batch_size):
        batch = acked_seqs[batch_start : batch_start + batch_size]
        for s in batch:
            key = f"dc:{run_id}:{s}"
            try:
                val = rc_verify.get(key)
                if val is None:
                    missing_keys.append(key)
                elif val != str(s):
                    missing_keys.append(key)
            except (ConnectionError, TimeoutError, ClusterDownError, RedisClusterException, OSError):
                verify_errors += 1

        if (batch_start // batch_size) % 20 == 0 and batch_start > 0:
            print(f"  Verified {batch_start + len(batch)}/{len(acked_seqs)}...")

    total_acked = len(acked_seqs)
    keys_missing = len(missing_keys)
    loss_rate = keys_missing / total_acked if total_acked > 0 else 0.0

    print(f"Verify done: {keys_missing} missing out of {total_acked} ACK'd "
          f"(loss rate: {loss_rate:.6f}), verify errors: {verify_errors}")

    # -- Build time series --
    ts_list = []
    for sec in sorted(writes_per_second.keys()):
        entry = writes_per_second[sec]
        ts_list.append({
            "second": sec,
            "attempted": entry["attempted"],
            "acked": entry["acked"],
            "failed": entry["failed"],
        })

    report = {
        "run_id": run_id,
        "host": host,
        "port": port,
        "duration_requested": duration,
        "duration_actual": round(write_duration, 2),
        "wall_start": wall_start,
        "total_attempted": seq,
        "total_acked": total_acked,
        "total_failed": len(failed_seqs),
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
    for s in acked_seqs:
        try:
            rc_verify.delete(f"dc:{run_id}:{s}")
            cleaned += 1
        except Exception:
            pass
    print(f"Cleaned {cleaned}/{total_acked} keys.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Valkey consistency checker")
    parser.add_argument("--host", default="valkey.vk.svc.cluster.local")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--duration", type=int, default=120,
                        help="Write phase duration in seconds")
    parser.add_argument("--run-id", default=None,
                        help="Unique run identifier (default: timestamp)")
    parser.add_argument("--output", type=Path, default=Path("/work/results/consistency_result.json"),
                        help="Output JSON path")
    args = parser.parse_args()

    if args.run_id is None:
        args.run_id = str(int(time.time()))

    run_check(
        host=args.host,
        port=args.port,
        duration=args.duration,
        run_id=args.run_id,
        output_path=args.output,
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Compute the pre-reshard steady-state throughput (ops/sec) from memtier
time-series in the resharding benchmark results.

memtier writes per-second buckets under ALL STATS -> Totals -> Time-Serie.
The reshard operation starts at `operation_start_s` (from the matching timing
file, default 30 s), so seconds [0, operation_start_s) are a clean baseline
collected under load BEFORE the cluster topology change begins.

Output: mean +/- std of the per-run baseline ops/sec, aggregated across runs,
per variant and direction.
"""
import csv
import json
import statistics
from pathlib import Path

BASE = Path(__file__).parent / "benchmark_results" / "reshard"

# variant -> (subdir, up_run_glob, up_timing_fmt, down_run_glob, down_timing_fmt)
VARIANTS = {
    "Memorystore": (
        "ms",
        "reshard_run_{i}.json", "reshard_timing_{i}.json",
        "reshard_down_run_{i}.json", "reshard_down_timing_{i}.json",
    ),
    "Valkey (legacy)": (
        "legacy",
        "reshard_legacy_run_{i}.json", "reshard_legacy_timing_{i}.json",
        "reshard_legacy_down_run_{i}.json", "reshard_legacy_down_timing_{i}.json",
    ),
    "Valkey (atomic)": (
        "atomic",
        "reshard_atomic_run_{i}.json", "reshard_atomic_timing_{i}.json",
        "reshard_atomic_down_run_{i}.json", "reshard_atomic_down_timing_{i}.json",
    ),
    "Redis 7.2": (
        "redis",
        "reshard_redis72_run_{i}.json", "reshard_redis72_timing_{i}.json",
        "reshard_redis72_down_run_{i}.json", "reshard_redis72_down_timing_{i}.json",
    ),
}

DEFAULT_OP_START = 30


def load_op_start(timing_path: Path) -> int:
    if timing_path.exists():
        try:
            t = json.loads(timing_path.read_text())
            if isinstance(t.get("operation_start_s"), (int, float)):
                return int(t["operation_start_s"])
        except (ValueError, OSError):
            pass
    return DEFAULT_OP_START


def baseline_ops_sec(run_path: Path, op_start: int):
    """Mean of per-second Totals Count over seconds [0, op_start)."""
    data = json.loads(run_path.read_text())
    series = data["ALL STATS"]["Totals"]["Time-Serie"]
    counts = []
    for sec in range(op_start):
        bucket = series.get(str(sec))
        if bucket and "Count" in bucket:
            counts.append(float(bucket["Count"]))
    if not counts:
        return None, 0
    return statistics.mean(counts), len(counts)


def collect(subdir, run_fmt, timing_fmt):
    """Return list of per-run baseline ops/sec for one variant+direction."""
    d = BASE / subdir
    out = []
    for i in range(1, 11):
        run_path = d / run_fmt.format(i=i)
        if not run_path.exists():
            continue
        op_start = load_op_start(d / timing_fmt.format(i=i))
        mean_ops, n = baseline_ops_sec(run_path, op_start)
        if mean_ops is not None:
            out.append((i, mean_ops, op_start, n))
    return out


def fmt(values):
    if not values:
        return "-", "-", 0
    m = statistics.mean(values)
    sd = statistics.stdev(values) if len(values) > 1 else 0.0
    return m, sd, len(values)


def main():
    rows = []
    print(f"{'Variant':18} {'Dir':5} {'baseline ops/sec (mean +/- std)':32} runs")
    print("-" * 70)
    for variant, (subdir, up_run, up_t, down_run, down_t) in VARIANTS.items():
        for direction, run_fmt, timing_fmt in (
            ("3->4", up_run, up_t),
            ("4->3", down_run, down_t),
        ):
            runs = collect(subdir, run_fmt, timing_fmt)
            vals = [r[1] for r in runs]
            m, sd, n = fmt(vals)
            if n:
                print(f"{variant:18} {direction:5} {m:>12.0f} +/- {sd:>9.0f}{'':9} {n}")
                rows.append({
                    "variant": variant,
                    "direction": direction,
                    "baseline_ops_sec_mean": round(m, 1),
                    "baseline_ops_sec_std": round(sd, 1),
                    "runs": n,
                    "window_s": runs[0][2] if runs else DEFAULT_OP_START,
                })
            else:
                print(f"{variant:18} {direction:5} {'(no data)':>32}")

    out_csv = Path(__file__).parent / "statistical_output" / "reshard_baseline_ops.csv"
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "variant", "direction", "baseline_ops_sec_mean",
            "baseline_ops_sec_std", "runs", "window_s",
        ])
        w.writeheader()
        w.writerows(rows)
    print(f"\nWritten: {out_csv}")


if __name__ == "__main__":
    main()

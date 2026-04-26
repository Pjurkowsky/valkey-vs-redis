from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

matplotlib.use("Agg")

BASELINE_SECONDS = 25
DROP_THRESHOLD = 0.50


def parse_time_series(run_result: Dict[str, Any]) -> pd.DataFrame:
    """Extract per-second time series from a single RUN result's Totals block."""
    totals = run_result.get("Totals", {})
    ts_raw = totals.get("Time-Serie", {})

    rows = []
    for sec_str in sorted(ts_raw.keys(), key=int):
        entry = ts_raw[sec_str]
        rows.append({
            "second": int(sec_str),
            "count": entry.get("Count", 0),
            "avg_latency": entry.get("Average Latency", float("nan")),
            "min_latency": entry.get("Min Latency", float("nan")),
            "max_latency": entry.get("Max Latency", float("nan")),
            "p50": entry.get("p50.00", float("nan")),
            "p95": entry.get("p95.00", float("nan")),
            "p99": entry.get("p99.00", float("nan")),
            "p999": entry.get("p99.90", float("nan")),
        })

    return pd.DataFrame(rows)


def detect_failover(ts: pd.DataFrame) -> Dict[str, Any]:
    """Analyse a single run's time series to detect the failover window.

    Returns a dict with failover metrics or None values if no failover detected.
    """
    if ts.empty:
        return _empty_result()

    available_baseline = min(BASELINE_SECONDS, len(ts) // 2)
    if available_baseline < 3:
        return _empty_result()

    baseline_ops = ts.loc[ts["second"] < available_baseline, "count"].mean()
    baseline_p99 = ts.loc[ts["second"] < BASELINE_SECONDS, "p99"].mean()

    if baseline_ops == 0:
        return _empty_result()

    threshold = baseline_ops * DROP_THRESHOLD
    degraded = ts[ts["count"] < threshold]

    if degraded.empty:
        return {
            "failover_detected": False,
            "baseline_ops": baseline_ops,
            "baseline_p99": baseline_p99,
            "failover_start_s": None,
            "failover_end_s": None,
            "failover_duration_s": 0.0,
            "failover_duration_ms": 0.0,
            "ops_lost": 0.0,
            "peak_p99_during": baseline_p99,
            "peak_max_latency_during": ts["max_latency"].max(),
        }

    failover_start = int(degraded["second"].min())
    failover_end = int(degraded["second"].max())
    duration_s = failover_end - failover_start + 1

    window = ts[(ts["second"] >= failover_start) & (ts["second"] <= failover_end)]
    ops_lost = max(0, baseline_ops * duration_s - window["count"].sum())
    peak_p99 = window["p99"].max()
    peak_max_lat = window["max_latency"].max()

    return {
        "failover_detected": True,
        "baseline_ops": baseline_ops,
        "baseline_p99": baseline_p99,
        "failover_start_s": failover_start,
        "failover_end_s": failover_end,
        "failover_duration_s": float(duration_s),
        "failover_duration_ms": float(duration_s * 1000),
        "ops_lost": ops_lost,
        "peak_p99_during": peak_p99,
        "peak_max_latency_during": peak_max_lat,
    }


def _empty_result() -> Dict[str, Any]:
    return {
        "failover_detected": False,
        "baseline_ops": None,
        "baseline_p99": None,
        "failover_start_s": None,
        "failover_end_s": None,
        "failover_duration_s": None,
        "failover_duration_ms": None,
        "ops_lost": None,
        "peak_p99_during": None,
        "peak_max_latency_during": None,
    }


def analyse_failover_runs(json_dir: Path) -> Tuple[pd.DataFrame, List[pd.DataFrame]]:
    """Parse all failover_run_*.json files and return (summary_df, list_of_timeseries)."""
    import json

    files = sorted(json_dir.glob("failover_run_*.json"))
    if not files:
        raise FileNotFoundError(f"No failover_run_*.json files in {json_dir}")

    results = []
    all_ts = []

    for f in files:
        with f.open() as fh:
            doc = json.load(fh)

        run_result = doc.get("RUN #1 RESULTS") or doc.get("ALL STATS")
        if run_result is None:
            for key in doc:
                if key.startswith("RUN #") and key.endswith("RESULTS"):
                    run_result = doc[key]
                    break

        if run_result is None:
            print(f"  [warn] No run results found in {f.name}, skipping")
            continue

        ts = parse_time_series(run_result)
        all_ts.append(ts)
        metrics = detect_failover(ts)
        metrics["file"] = f.name
        results.append(metrics)

    return pd.DataFrame(results), all_ts


def print_failover_summary(df: pd.DataFrame) -> None:
    """Print aggregated failover statistics."""
    detected = df[df["failover_detected"] == True]

    print(f"\nFailover runs analysed: {len(df)}")
    print(f"Failover detected in:  {len(detected)}/{len(df)} runs")

    if detected.empty:
        print("No failover events detected.")
        return

    metrics = [
        ("Failover duration (ms)", "failover_duration_ms"),
        ("Ops lost", "ops_lost"),
        ("Baseline ops/sec", "baseline_ops"),
        ("Peak p99 during failover (ms)", "peak_p99_during"),
        ("Baseline p99 (ms)", "baseline_p99"),
    ]

    print(f"\n{'Metric':<40} {'Mean':>12} {'Std':>12}")
    print("-" * 66)
    for label, col in metrics:
        mean = detected[col].mean()
        std = detected[col].std()
        print(f"{label:<40} {mean:>12.2f} {std:>12.2f}")


def save_failover_csv(df: pd.DataFrame, out_dir: Path) -> None:
    csv_path = out_dir / "failover_summary.csv"
    df.to_csv(csv_path, index=False)
    print(f"\nFailover CSV saved to {csv_path}")


def plot_failover_timeseries(
    all_ts: List[pd.DataFrame],
    results_df: pd.DataFrame,
    out_dir: Path,
) -> None:
    """Plot ops/sec over time for each failover run, highlighting the failover window."""
    for i, (ts, (_, row)) in enumerate(zip(all_ts, results_df.iterrows())):
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6), sharex=True)

        ax1.plot(ts["second"], ts["count"], linewidth=0.8, color="#4c72b0")
        ax1.set_ylabel("Ops/sec")
        ax1.set_title(f"Failover Run {i + 1}")

        if row.get("failover_detected") and row["failover_start_s"] is not None:
            ax1.axvspan(
                row["failover_start_s"], row["failover_end_s"],
                alpha=0.2, color="red", label="Failover window",
            )
            ax1.axhline(
                row["baseline_ops"], linestyle="--", color="gray",
                linewidth=0.7, label="Baseline",
            )
            ax1.legend(fontsize=8)

        ax2.plot(ts["second"], ts["p99"], linewidth=0.8, color="#c44e52", label="p99")
        ax2.plot(ts["second"], ts["p50"], linewidth=0.8, color="#55a868", label="p50")
        ax2.set_ylabel("Latency (ms)")
        ax2.set_xlabel("Time (s)")
        ax2.legend(fontsize=8)

        if row.get("failover_detected") and row["failover_start_s"] is not None:
            ax2.axvspan(
                row["failover_start_s"], row["failover_end_s"],
                alpha=0.2, color="red",
            )

        fig.tight_layout()
        fig.savefig(out_dir / f"failover_run_{i + 1}.png", dpi=150, bbox_inches="tight")
        plt.close(fig)


def plot_failover_comparison(results_df: pd.DataFrame, out_dir: Path) -> None:
    """Bar chart comparing failover duration across runs."""
    detected = results_df[results_df["failover_detected"] == True]
    if detected.empty:
        return

    fig, ax = plt.subplots(figsize=(6, 4))
    x = np.arange(len(detected))
    ax.bar(x, detected["failover_duration_ms"], color="#dd8452", edgecolor="black", linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels([f"Run {i+1}" for i in range(len(detected))])
    ax.set_ylabel("Failover Duration (ms)")
    ax.set_title("Failover Duration Across Runs")

    mean_val = detected["failover_duration_ms"].mean()
    ax.axhline(mean_val, linestyle="--", color="gray", linewidth=0.7,
               label=f"Mean: {mean_val:.0f} ms")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_dir / "failover_comparison.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

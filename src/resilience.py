from pathlib import Path
from typing import Any, Dict, List, Tuple

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.failover import parse_time_series

matplotlib.use("Agg")

BASELINE_SECONDS = 25
DEGRADATION_THRESHOLD = 0.80
RECOVERY_THRESHOLD = 0.90


def detect_degradation(ts: pd.DataFrame) -> Dict[str, Any]:
    """Analyse a time series for performance degradation under stress.

    Unlike failover detection (which looks for ops dropping to ~0), this looks
    for partial degradation and whether the system recovers after stress ends.
    """
    if ts.empty:
        return _empty_result()

    available_baseline = min(BASELINE_SECONDS, len(ts) // 2)
    if available_baseline < 3:
        return _empty_result()

    baseline = ts[ts["second"] < available_baseline]
    baseline_ops = baseline["count"].mean()
    baseline_p99 = baseline["p99"].mean()
    baseline_p999 = baseline["p999"].mean()

    if baseline_ops == 0:
        return _empty_result()

    threshold = baseline_ops * DEGRADATION_THRESHOLD
    degraded = ts[(ts["second"] >= available_baseline) & (ts["count"] < threshold)]

    if degraded.empty:
        return {
            "degradation_detected": False,
            "baseline_ops": baseline_ops,
            "baseline_p99": baseline_p99,
            "baseline_p999": baseline_p999,
            "degradation_start_s": None,
            "degradation_end_s": None,
            "degradation_duration_s": 0.0,
            "degradation_duration_ms": 0.0,
            "min_ops_during": ts["count"].min(),
            "ops_drop_pct": 0.0,
            "peak_p99_during": ts["p99"].max(),
            "peak_p999_during": ts["p999"].max(),
            "recovery_detected": True,
            "pod_restart_detected": False,
        }

    deg_start = int(degraded["second"].min())
    deg_end = int(degraded["second"].max())
    duration_s = deg_end - deg_start + 1

    window = ts[(ts["second"] >= deg_start) & (ts["second"] <= deg_end)]
    min_ops = window["count"].min()
    ops_drop_pct = (1.0 - min_ops / baseline_ops) * 100.0
    peak_p99 = window["p99"].max()
    peak_p999 = window["p999"].max()

    zero_ops_seconds = (window["count"] == 0).sum()
    pod_restart = zero_ops_seconds >= 2

    recovery_threshold_ops = baseline_ops * RECOVERY_THRESHOLD
    post_stress = ts[ts["second"] > deg_end]
    if post_stress.empty:
        recovery = False
    else:
        last_10 = post_stress.tail(min(10, len(post_stress)))
        recovery = last_10["count"].mean() >= recovery_threshold_ops

    return {
        "degradation_detected": True,
        "baseline_ops": baseline_ops,
        "baseline_p99": baseline_p99,
        "baseline_p999": baseline_p999,
        "degradation_start_s": deg_start,
        "degradation_end_s": deg_end,
        "degradation_duration_s": float(duration_s),
        "degradation_duration_ms": float(duration_s * 1000),
        "min_ops_during": float(min_ops),
        "ops_drop_pct": ops_drop_pct,
        "peak_p99_during": peak_p99,
        "peak_p999_during": peak_p999,
        "recovery_detected": recovery,
        "pod_restart_detected": pod_restart,
    }


def _empty_result() -> Dict[str, Any]:
    return {
        "degradation_detected": False,
        "baseline_ops": None,
        "baseline_p99": None,
        "baseline_p999": None,
        "degradation_start_s": None,
        "degradation_end_s": None,
        "degradation_duration_s": None,
        "degradation_duration_ms": None,
        "min_ops_during": None,
        "ops_drop_pct": None,
        "peak_p99_during": None,
        "peak_p999_during": None,
        "recovery_detected": None,
        "pod_restart_detected": None,
    }


def analyse_resilience_runs(
    json_dir: Path, scenario: str,
) -> Tuple[pd.DataFrame, List[pd.DataFrame]]:
    """Parse resilience_{scenario}_run_*.json files."""
    import json

    prefix = "resilience_cpu" if scenario == "cpu" else "resilience_mem"
    files = sorted(json_dir.glob(f"{prefix}_run_*.json"))
    if not files:
        raise FileNotFoundError(
            f"No {prefix}_run_*.json files in {json_dir}"
        )

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
        metrics = detect_degradation(ts)
        metrics["file"] = f.name
        metrics["scenario"] = scenario
        results.append(metrics)

    return pd.DataFrame(results), all_ts


def print_resilience_summary(df: pd.DataFrame, scenario: str) -> None:
    detected = df[df["degradation_detected"] == True]
    recovered = df[df["recovery_detected"] == True]
    restarts = df[df["pod_restart_detected"] == True]

    print(f"\nResilience [{scenario}] runs analysed: {len(df)}")
    print(f"Degradation detected: {len(detected)}/{len(df)} runs")
    print(f"Recovery after stress: {len(recovered)}/{len(df)} runs")
    print(f"Pod restart (OOM):     {len(restarts)}/{len(df)} runs")

    if detected.empty:
        print("No degradation events detected.")
        return

    metrics = [
        ("Degradation duration (ms)", "degradation_duration_ms"),
        ("Min ops/sec during stress", "min_ops_during"),
        ("Ops drop (%)", "ops_drop_pct"),
        ("Baseline ops/sec", "baseline_ops"),
        ("Peak p99 during stress (ms)", "peak_p99_during"),
        ("Baseline p99 (ms)", "baseline_p99"),
        ("Peak p99.9 during stress (ms)", "peak_p999_during"),
        ("Baseline p99.9 (ms)", "baseline_p999"),
    ]

    print(f"\n{'Metric':<40} {'Mean':>12} {'Std':>12}")
    print("-" * 66)
    for label, col in metrics:
        mean = detected[col].mean()
        std = detected[col].std()
        print(f"{label:<40} {mean:>12.2f} {std:>12.2f}")


def save_resilience_csv(df: pd.DataFrame, out_dir: Path, scenario: str) -> None:
    csv_path = out_dir / f"resilience_{scenario}_summary.csv"
    df.to_csv(csv_path, index=False)
    print(f"\nResilience CSV saved to {csv_path}")


def plot_resilience_timeseries(
    all_ts: List[pd.DataFrame],
    results_df: pd.DataFrame,
    out_dir: Path,
    scenario: str,
) -> None:
    """Plot ops/sec and latency over time, highlighting degradation window."""
    label = "CPU stress" if scenario == "cpu" else "Memory stress"

    for i, (ts, (_, row)) in enumerate(zip(all_ts, results_df.iterrows())):
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6), sharex=True)

        ax1.plot(ts["second"], ts["count"], linewidth=0.8, color="#4c72b0")
        ax1.set_ylabel("Ops/sec")
        ax1.set_title(f"Resilience [{label}] Run {i + 1}")

        if row.get("degradation_detected") and row["degradation_start_s"] is not None:
            ax1.axvspan(
                row["degradation_start_s"], row["degradation_end_s"],
                alpha=0.2, color="orange", label="Degradation window",
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

        if row.get("degradation_detected") and row["degradation_start_s"] is not None:
            ax2.axvspan(
                row["degradation_start_s"], row["degradation_end_s"],
                alpha=0.2, color="orange",
            )

        fig.tight_layout()
        prefix = "cpu" if scenario == "cpu" else "mem"
        fig.savefig(
            out_dir / f"resilience_{prefix}_run_{i + 1}.png",
            dpi=150, bbox_inches="tight",
        )
        plt.close(fig)


def plot_resilience_comparison(
    results_df: pd.DataFrame, out_dir: Path, scenario: str,
) -> None:
    """Bar chart comparing degradation severity across runs."""
    detected = results_df[results_df["degradation_detected"] == True]
    if detected.empty:
        return

    label = "CPU stress" if scenario == "cpu" else "Memory stress"
    prefix = "cpu" if scenario == "cpu" else "mem"

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))

    x = np.arange(len(detected))

    ax1.bar(x, detected["ops_drop_pct"], color="#dd8452", edgecolor="black", linewidth=0.5)
    ax1.set_xticks(x)
    ax1.set_xticklabels([f"Run {j+1}" for j in range(len(detected))])
    ax1.set_ylabel("Ops drop (%)")
    ax1.set_title(f"Ops/sec drop  |  {label}")
    mean_drop = detected["ops_drop_pct"].mean()
    ax1.axhline(mean_drop, linestyle="--", color="gray", linewidth=0.7,
                label=f"Mean: {mean_drop:.1f}%")
    ax1.legend(fontsize=8)

    ax2.bar(x, detected["peak_p99_during"], color="#c44e52", edgecolor="black", linewidth=0.5,
            label="Peak p99 (stress)")
    ax2.bar(x, detected["baseline_p99"], color="#55a868", edgecolor="black", linewidth=0.5,
            alpha=0.5, label="Baseline p99")
    ax2.set_xticks(x)
    ax2.set_xticklabels([f"Run {j+1}" for j in range(len(detected))])
    ax2.set_ylabel("Latency (ms)")
    ax2.set_title(f"p99 latency  |  {label}")
    ax2.legend(fontsize=8)

    fig.tight_layout()
    fig.savefig(
        out_dir / f"resilience_{prefix}_comparison.png",
        dpi=150, bbox_inches="tight",
    )
    plt.close(fig)

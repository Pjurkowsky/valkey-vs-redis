from pathlib import Path
from typing import Any, Dict, List, Tuple

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.failover import parse_time_series

matplotlib.use("Agg")

BASELINE_SECONDS = 25
DISRUPTION_THRESHOLD = 0.80
CLEAN_DISRUPTION_MAX_S = 2


def detect_disruptions(ts: pd.DataFrame) -> Dict[str, Any]:
    """Find all disruption windows during a rolling upgrade.

    A rolling update can cause multiple short dips (one per pod restart)
    rather than a single failover window.
    """
    if ts.empty:
        return _empty_result()

    available_baseline = min(BASELINE_SECONDS, len(ts) // 2)
    if available_baseline < 3:
        return _empty_result()

    baseline = ts[ts["second"] < available_baseline]
    baseline_ops = baseline["count"].mean()
    baseline_p99 = baseline["p99"].mean()

    if baseline_ops == 0:
        return _empty_result()

    threshold = baseline_ops * DISRUPTION_THRESHOLD

    is_degraded = (ts["count"] < threshold) & (ts["second"] >= available_baseline)
    ts = ts.copy()
    ts["degraded"] = is_degraded

    windows: List[Dict[str, Any]] = []
    in_window = False
    win_start = 0

    for _, row in ts.iterrows():
        if row["degraded"] and not in_window:
            in_window = True
            win_start = int(row["second"])
        elif not row["degraded"] and in_window:
            in_window = False
            win_end = int(row["second"]) - 1
            windows.append({"start": win_start, "end": win_end, "duration_s": win_end - win_start + 1})

    if in_window:
        win_end = int(ts["second"].max())
        windows.append({"start": win_start, "end": win_end, "duration_s": win_end - win_start + 1})

    if not windows:
        return {
            "disruptions_detected": 0,
            "baseline_ops": baseline_ops,
            "baseline_p99": baseline_p99,
            "total_disrupted_s": 0,
            "total_disrupted_ms": 0,
            "total_ops_lost": 0.0,
            "max_single_disruption_s": 0,
            "max_single_disruption_ms": 0,
            "peak_p99_during": ts["p99"].max(),
            "upgrade_clean": True,
            "windows": [],
        }

    total_disrupted_s = sum(w["duration_s"] for w in windows)
    max_single = max(w["duration_s"] for w in windows)

    total_ops_lost = 0.0
    peak_p99 = 0.0
    for w in windows:
        win_ts = ts[(ts["second"] >= w["start"]) & (ts["second"] <= w["end"])]
        total_ops_lost += max(0, baseline_ops * w["duration_s"] - win_ts["count"].sum())
        win_peak = win_ts["p99"].max()
        if win_peak > peak_p99:
            peak_p99 = win_peak

    upgrade_clean = all(w["duration_s"] <= CLEAN_DISRUPTION_MAX_S for w in windows)

    return {
        "disruptions_detected": len(windows),
        "baseline_ops": baseline_ops,
        "baseline_p99": baseline_p99,
        "total_disrupted_s": total_disrupted_s,
        "total_disrupted_ms": total_disrupted_s * 1000,
        "total_ops_lost": total_ops_lost,
        "max_single_disruption_s": max_single,
        "max_single_disruption_ms": max_single * 1000,
        "peak_p99_during": peak_p99,
        "upgrade_clean": upgrade_clean,
        "windows": windows,
    }


def _empty_result() -> Dict[str, Any]:
    return {
        "disruptions_detected": None,
        "baseline_ops": None,
        "baseline_p99": None,
        "total_disrupted_s": None,
        "total_disrupted_ms": None,
        "total_ops_lost": None,
        "max_single_disruption_s": None,
        "max_single_disruption_ms": None,
        "peak_p99_during": None,
        "upgrade_clean": None,
        "windows": [],
    }


def analyse_upgrade_runs(json_dir: Path) -> Tuple[pd.DataFrame, List[pd.DataFrame], List[List[Dict]]]:
    """Parse all upgrade_run_*.json files."""
    import json

    files = sorted(json_dir.glob("upgrade_run_*.json"))
    if not files:
        raise FileNotFoundError(f"No upgrade_run_*.json files in {json_dir}")

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
        metrics = detect_disruptions(ts)

        row = {k: v for k, v in metrics.items() if k != "windows"}
        row["file"] = f.name
        row["_windows"] = metrics["windows"]
        results.append(row)

    df = pd.DataFrame([{k: v for k, v in r.items() if k != "_windows"} for r in results])
    all_windows = [r["_windows"] for r in results]

    return df, all_ts, all_windows


def print_upgrade_summary(df: pd.DataFrame) -> None:
    has_data = df["disruptions_detected"].notna()
    valid = df[has_data]

    print(f"\nUpgrade runs analysed: {len(df)}")
    if valid.empty:
        print("No valid data found.")
        return

    total_disruptions = valid["disruptions_detected"].sum()
    clean_count = valid["upgrade_clean"].sum()

    print(f"Total disruption events: {int(total_disruptions)} across {len(valid)} runs")
    print(f"Clean upgrades (<={CLEAN_DISRUPTION_MAX_S}s per dip): {int(clean_count)}/{len(valid)}")

    metrics = [
        ("Disruption events per run", "disruptions_detected"),
        ("Total disrupted time (ms)", "total_disrupted_ms"),
        ("Max single disruption (ms)", "max_single_disruption_ms"),
        ("Total ops lost", "total_ops_lost"),
        ("Baseline ops/sec", "baseline_ops"),
        ("Peak p99 during upgrade (ms)", "peak_p99_during"),
        ("Baseline p99 (ms)", "baseline_p99"),
    ]

    print(f"\n{'Metric':<40} {'Mean':>12} {'Std':>12}")
    print("-" * 66)
    for label, col in metrics:
        mean = valid[col].mean()
        std = valid[col].std()
        print(f"{label:<40} {mean:>12.2f} {std:>12.2f}")


def save_upgrade_csv(df: pd.DataFrame, out_dir: Path) -> None:
    csv_path = out_dir / "upgrade_summary.csv"
    df.to_csv(csv_path, index=False)
    print(f"\nUpgrade CSV saved to {csv_path}")


def plot_upgrade_timeseries(
    all_ts: List[pd.DataFrame],
    all_windows: List[List[Dict]],
    results_df: pd.DataFrame,
    out_dir: Path,
) -> None:
    """Plot ops/sec and latency over time, highlighting all disruption windows."""
    for i, (ts, windows, (_, row)) in enumerate(
        zip(all_ts, all_windows, results_df.iterrows())
    ):
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 6), sharex=True)

        ax1.plot(ts["second"], ts["count"], linewidth=0.6, color="#4c72b0")
        ax1.set_ylabel("Ops/sec")
        ax1.set_title(f"Rolling Upgrade Run {i + 1}")

        if row.get("baseline_ops") is not None:
            ax1.axhline(
                row["baseline_ops"], linestyle="--", color="gray",
                linewidth=0.7, label="Baseline",
            )

        for j, w in enumerate(windows):
            ax1.axvspan(
                w["start"], w["end"],
                alpha=0.2, color="orange",
                label="Disruption" if j == 0 else None,
            )

        if windows:
            ax1.legend(fontsize=8)

        ax2.plot(ts["second"], ts["p99"], linewidth=0.6, color="#c44e52", label="p99")
        ax2.plot(ts["second"], ts["p50"], linewidth=0.6, color="#55a868", label="p50")
        ax2.set_ylabel("Latency (ms)")
        ax2.set_xlabel("Time (s)")
        ax2.legend(fontsize=8)

        for w in windows:
            ax2.axvspan(w["start"], w["end"], alpha=0.2, color="orange")

        fig.tight_layout()
        fig.savefig(out_dir / f"upgrade_run_{i + 1}.png", dpi=150, bbox_inches="tight")
        plt.close(fig)


def plot_upgrade_comparison(results_df: pd.DataFrame, out_dir: Path) -> None:
    """Bar chart comparing disruption severity across runs."""
    valid = results_df[results_df["disruptions_detected"].notna()]
    if valid.empty:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
    x = np.arange(len(valid))

    ax1.bar(x, valid["total_disrupted_ms"], color="#dd8452",
            edgecolor="black", linewidth=0.5)
    ax1.set_xticks(x)
    ax1.set_xticklabels([f"Run {j+1}" for j in range(len(valid))])
    ax1.set_ylabel("Total disrupted time (ms)")
    ax1.set_title("Total disruption during upgrade")
    mean_val = valid["total_disrupted_ms"].mean()
    ax1.axhline(mean_val, linestyle="--", color="gray", linewidth=0.7,
                label=f"Mean: {mean_val:.0f} ms")
    ax1.legend(fontsize=8)

    ax2.bar(x, valid["disruptions_detected"], color="#4c72b0",
            edgecolor="black", linewidth=0.5)
    ax2.set_xticks(x)
    ax2.set_xticklabels([f"Run {j+1}" for j in range(len(valid))])
    ax2.set_ylabel("Number of disruption events")
    ax2.set_title("Disruption events per upgrade")

    fig.tight_layout()
    fig.savefig(out_dir / "upgrade_comparison.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

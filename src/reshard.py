"""Analysis module for horizontal scaling / resharding tests."""

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.failover import parse_time_series
from src.upgrade import detect_disruptions

matplotlib.use("Agg")


def _load_timing(path: Path) -> Dict[str, Any]:
    with path.open() as f:
        return json.load(f)


def analyse_reshard_runs(
    json_dir: Path,
) -> Tuple[pd.DataFrame, List[pd.DataFrame], List[List[Dict]], List[Dict]]:
    """Parse reshard_run_*.json (memtier) + reshard_timing_*.json files.

    Returns (summary_df, list_of_timeseries, list_of_disruption_windows, list_of_timings).
    """
    memtier_files = sorted(json_dir.glob("reshard_run_*.json"))
    if not memtier_files:
        raise FileNotFoundError(f"No reshard_run_*.json files in {json_dir}")

    rows = []
    all_ts: List[pd.DataFrame] = []
    all_windows: List[List[Dict]] = []
    all_timings: List[Dict] = []

    for f in memtier_files:
        run_num = f.stem.split("_")[-1]
        timing_path = json_dir / f"reshard_timing_{run_num}.json"

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

        disruption = detect_disruptions(ts)
        windows = disruption.get("windows", [])
        all_windows.append(windows)

        timing = {}
        if timing_path.exists():
            timing = _load_timing(timing_path)
        all_timings.append(timing)

        row = {k: v for k, v in disruption.items() if k != "windows"}
        row["file"] = f.name
        row["rebalance_duration_s"] = timing.get("rebalance_duration_s")
        row["scale_duration_s"] = timing.get("scale_duration_s")
        row["masters_after"] = timing.get("masters_after")
        rows.append(row)

    df = pd.DataFrame(rows)
    return df, all_ts, all_windows, all_timings


def print_reshard_summary(df: pd.DataFrame) -> None:
    has_data = df["disruptions_detected"].notna()
    valid = df[has_data]

    print(f"\nReshard runs analysed: {len(df)}")
    if valid.empty:
        print("No valid data found.")
        return

    total_disruptions = valid["disruptions_detected"].sum()
    print(f"Total disruption events: {int(total_disruptions)} across {len(valid)} runs")

    metrics = [
        ("Rebalance duration (s)", "rebalance_duration_s"),
        ("Scale-up duration (s)", "scale_duration_s"),
        ("Disruption events per run", "disruptions_detected"),
        ("Total disrupted time (ms)", "total_disrupted_ms"),
        ("Max single disruption (ms)", "max_single_disruption_ms"),
        ("Total ops lost", "total_ops_lost"),
        ("Baseline ops/sec", "baseline_ops"),
        ("Peak p99 during reshard (ms)", "peak_p99_during"),
        ("Baseline p99 (ms)", "baseline_p99"),
    ]

    print(f"\n{'Metric':<40} {'Mean':>12} {'Std':>12}")
    print("-" * 66)
    for label, col in metrics:
        col_data = valid[col].dropna()
        if col_data.empty:
            continue
        mean = col_data.mean()
        std = col_data.std()
        print(f"{label:<40} {mean:>12.2f} {std:>12.2f}")


def save_reshard_csv(df: pd.DataFrame, out_dir: Path) -> None:
    csv_path = out_dir / "reshard_summary.csv"
    df.to_csv(csv_path, index=False)
    print(f"\nReshard CSV saved to {csv_path}")


def plot_reshard_timeseries(
    all_ts: List[pd.DataFrame],
    all_windows: List[List[Dict]],
    all_timings: List[Dict],
    results_df: pd.DataFrame,
    out_dir: Path,
) -> None:
    """Plot ops/sec and latency over time, highlighting disruption windows."""
    for i, (ts, windows, timing, (_, row)) in enumerate(
        zip(all_ts, all_windows, all_timings, results_df.iterrows())
    ):
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 6), sharex=True)

        ax1.plot(ts["second"], ts["count"], linewidth=0.6, color="#4c72b0")
        ax1.set_ylabel("Ops/sec")

        rebal_dur = timing.get("rebalance_duration_s", "?")
        ax1.set_title(f"Reshard Run {i + 1} (rebalance: {rebal_dur}s)")

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

        if windows or row.get("baseline_ops") is not None:
            ax1.legend(fontsize=8)

        ax2.plot(ts["second"], ts["p99"], linewidth=0.6, color="#c44e52", label="p99")
        ax2.plot(ts["second"], ts["p50"], linewidth=0.6, color="#55a868", label="p50")
        ax2.set_ylabel("Latency (ms)")
        ax2.set_xlabel("Time (s)")
        ax2.legend(fontsize=8)

        for w in windows:
            ax2.axvspan(w["start"], w["end"], alpha=0.2, color="orange")

        fig.tight_layout()
        fig.savefig(out_dir / f"reshard_run_{i + 1}.png", dpi=150, bbox_inches="tight")
        plt.close(fig)


def plot_reshard_comparison(results_df: pd.DataFrame, out_dir: Path) -> None:
    """Bar charts comparing rebalance duration and disruption across runs."""
    valid = results_df[results_df["disruptions_detected"].notna()]
    if valid.empty:
        return

    has_timing = valid["rebalance_duration_s"].notna().any()
    ncols = 3 if has_timing else 2
    fig, axes = plt.subplots(1, ncols, figsize=(5 * ncols, 4))
    x = np.arange(len(valid))

    ax_idx = 0

    if has_timing:
        axes[ax_idx].bar(x, valid["rebalance_duration_s"].fillna(0),
                         color="#55a868", edgecolor="black", linewidth=0.5)
        axes[ax_idx].set_xticks(x)
        axes[ax_idx].set_xticklabels([f"Run {j+1}" for j in range(len(valid))])
        axes[ax_idx].set_ylabel("Rebalance duration (s)")
        axes[ax_idx].set_title("Rebalance Duration")
        mean_rebal = valid["rebalance_duration_s"].dropna().mean()
        axes[ax_idx].axhline(mean_rebal, linestyle="--", color="gray",
                             linewidth=0.7, label=f"Mean: {mean_rebal:.0f}s")
        axes[ax_idx].legend(fontsize=8)
        ax_idx += 1

    axes[ax_idx].bar(x, valid["total_disrupted_ms"], color="#dd8452",
                     edgecolor="black", linewidth=0.5)
    axes[ax_idx].set_xticks(x)
    axes[ax_idx].set_xticklabels([f"Run {j+1}" for j in range(len(valid))])
    axes[ax_idx].set_ylabel("Total disrupted time (ms)")
    axes[ax_idx].set_title("Traffic Disruption")
    mean_dis = valid["total_disrupted_ms"].mean()
    axes[ax_idx].axhline(mean_dis, linestyle="--", color="gray",
                         linewidth=0.7, label=f"Mean: {mean_dis:.0f} ms")
    axes[ax_idx].legend(fontsize=8)
    ax_idx += 1

    axes[ax_idx].bar(x, valid["total_ops_lost"], color="#4c72b0",
                     edgecolor="black", linewidth=0.5)
    axes[ax_idx].set_xticks(x)
    axes[ax_idx].set_xticklabels([f"Run {j+1}" for j in range(len(valid))])
    axes[ax_idx].set_ylabel("Ops lost")
    axes[ax_idx].set_title("Operations Lost During Reshard")

    fig.tight_layout()
    fig.savefig(out_dir / "reshard_comparison.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

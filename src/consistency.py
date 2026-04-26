"""Analysis module for data consistency (network partition) tests."""

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

matplotlib.use("Agg")


def _parse_report(path: Path) -> Dict[str, Any]:
    with path.open() as f:
        return json.load(f)


def analyse_consistency_runs(
    json_dir: Path,
) -> Tuple[pd.DataFrame, List[List[Dict]]]:
    """Parse all consistency_run_*.json files.

    Returns (summary_df, list_of_write_rate_timeseries).
    """
    files = sorted(json_dir.glob("consistency_run_*.json"))
    if not files:
        raise FileNotFoundError(f"No consistency_run_*.json files in {json_dir}")

    rows = []
    all_ts: List[List[Dict]] = []

    for f in files:
        report = _parse_report(f)
        ts = report.get("write_rate_per_second", [])
        all_ts.append(ts)

        total_acked = report.get("total_acked", 0)
        keys_missing = report.get("keys_missing", 0)

        acked_rate = []
        failed_rate = []
        for entry in ts:
            acked_rate.append(entry.get("acked", 0))
            failed_rate.append(entry.get("failed", 0))

        rows.append({
            "file": f.name,
            "run_id": report.get("run_id", ""),
            "total_attempted": report.get("total_attempted", 0),
            "total_acked": total_acked,
            "total_failed": report.get("total_failed", 0),
            "keys_missing": keys_missing,
            "loss_rate": report.get("loss_rate", 0.0),
            "verify_errors": report.get("verify_errors", 0),
            "duration_actual": report.get("duration_actual", 0),
            "write_rate_mean": np.mean(acked_rate) if acked_rate else 0,
            "partition_errors": sum(failed_rate),
        })

    return pd.DataFrame(rows), all_ts


def print_consistency_summary(df: pd.DataFrame) -> None:
    print(f"\nConsistency runs analysed: {len(df)}")

    total_lost = int(df["keys_missing"].sum())
    total_acked = int(df["total_acked"].sum())
    runs_with_loss = int((df["keys_missing"] > 0).sum())

    print(f"Runs with data loss: {runs_with_loss}/{len(df)}")
    print(f"Total keys lost:     {total_lost} / {total_acked} ACK'd "
          f"({total_lost / total_acked * 100:.4f}%)" if total_acked > 0 else "")

    metrics = [
        ("Keys missing (per run)", "keys_missing"),
        ("Loss rate", "loss_rate"),
        ("Total ACK'd writes", "total_acked"),
        ("Total failed writes", "total_failed"),
        ("Partition errors", "partition_errors"),
        ("Write rate mean (ops/s)", "write_rate_mean"),
        ("Verify errors", "verify_errors"),
    ]

    print(f"\n{'Metric':<35} {'Mean':>12} {'Std':>12}")
    print("-" * 61)
    for label, col in metrics:
        mean = df[col].mean()
        std = df[col].std()
        print(f"{label:<35} {mean:>12.2f} {std:>12.2f}")


def save_consistency_csv(df: pd.DataFrame, out_dir: Path) -> None:
    csv_path = out_dir / "consistency_summary.csv"
    df.to_csv(csv_path, index=False)
    print(f"\nConsistency CSV saved to {csv_path}")


def plot_consistency_timeseries(
    all_ts: List[List[Dict]],
    results_df: pd.DataFrame,
    out_dir: Path,
) -> None:
    """Plot write rate over time for each run, showing failed writes."""
    for i, (ts, (_, row)) in enumerate(zip(all_ts, results_df.iterrows())):
        if not ts:
            continue

        seconds = [e["second"] for e in ts]
        acked = [e["acked"] for e in ts]
        failed = [e["failed"] for e in ts]

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 6), sharex=True)

        ax1.plot(seconds, acked, linewidth=0.7, color="#4c72b0", label="ACK'd writes/s")
        ax1.set_ylabel("Writes/sec (ACK'd)")
        loss_info = f" | lost={row['keys_missing']}" if row["keys_missing"] > 0 else " | no loss"
        ax1.set_title(f"Consistency Run {i + 1}{loss_info}")
        ax1.legend(fontsize=8)

        has_failures = any(f > 0 for f in failed)
        if has_failures:
            fail_start = None
            for j, f in enumerate(failed):
                if f > 0 and fail_start is None:
                    fail_start = seconds[j]
                elif f == 0 and fail_start is not None:
                    ax1.axvspan(fail_start, seconds[j - 1], alpha=0.15, color="red",
                                label="Partition errors" if fail_start == seconds[[k for k, v in enumerate(failed) if v > 0][0]] else None)
                    fail_start = None
            if fail_start is not None:
                ax1.axvspan(fail_start, seconds[-1], alpha=0.15, color="red")
            ax1.legend(fontsize=8)

        ax2.bar(seconds, failed, width=1.0, color="#c44e52", alpha=0.7)
        ax2.set_ylabel("Failed writes/sec")
        ax2.set_xlabel("Time (s)")

        fig.tight_layout()
        fig.savefig(out_dir / f"consistency_run_{i + 1}.png", dpi=150, bbox_inches="tight")
        plt.close(fig)


def plot_consistency_comparison(results_df: pd.DataFrame, out_dir: Path) -> None:
    """Bar charts comparing loss rate and partition errors across runs."""
    if results_df.empty:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
    x = np.arange(len(results_df))

    ax1.bar(x, results_df["keys_missing"], color="#c44e52",
            edgecolor="black", linewidth=0.5)
    ax1.set_xticks(x)
    ax1.set_xticklabels([f"Run {j + 1}" for j in range(len(results_df))])
    ax1.set_ylabel("Keys lost (ACK'd but missing)")
    ax1.set_title("Data Loss per Run")
    if results_df["keys_missing"].sum() > 0:
        mean_val = results_df["keys_missing"].mean()
        ax1.axhline(mean_val, linestyle="--", color="gray", linewidth=0.7,
                     label=f"Mean: {mean_val:.1f}")
        ax1.legend(fontsize=8)

    ax2.bar(x, results_df["partition_errors"], color="#dd8452",
            edgecolor="black", linewidth=0.5)
    ax2.set_xticks(x)
    ax2.set_xticklabels([f"Run {j + 1}" for j in range(len(results_df))])
    ax2.set_ylabel("Write failures during partition")
    ax2.set_title("Partition Errors per Run")

    fig.tight_layout()
    fig.savefig(out_dir / "consistency_comparison.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

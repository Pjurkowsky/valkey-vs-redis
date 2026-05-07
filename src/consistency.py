"""Analysis module for data consistency (network partition) tests."""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.chart_markers import mark_test_window

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
        total_attempted = report.get("total_attempted", 0)
        total_failed = report.get("total_failed", 0)
        total_slow = report.get("total_slow", 0)
        total_affected = report.get("total_affected", total_failed + total_slow)
        keys_missing = report.get("keys_missing", 0)

        acked_rate = []
        failed_rate = []
        affected_rate = []
        for entry in ts:
            acked_rate.append(entry.get("acked", 0))
            failed_rate.append(entry.get("failed", 0))
            attempted = entry.get("attempted", 0)
            affected = entry.get("affected", entry.get("failed", 0) + entry.get("slow", 0))
            affected_rate.append(affected / attempted if attempted > 0 else 0)

        rows.append({
            "file": f.name,
            "run_id": report.get("run_id", ""),
            "clients": report.get("clients", 1),
            "socket_timeout": report.get("socket_timeout", None),
            "socket_connect_timeout": report.get("socket_connect_timeout", None),
            "slow_threshold_ms": report.get("slow_threshold_ms", None),
            "total_attempted": total_attempted,
            "total_acked": total_acked,
            "total_failed": total_failed,
            "total_slow": total_slow,
            "total_affected": total_affected,
            "affected_rate": report.get(
                "affected_rate",
                total_affected / total_attempted if total_attempted > 0 else 0.0,
            ),
            "failed_rate": report.get(
                "failed_rate",
                total_failed / total_attempted if total_attempted > 0 else 0.0,
            ),
            "slow_rate": report.get(
                "slow_rate",
                total_slow / total_attempted if total_attempted > 0 else 0.0,
            ),
            "keys_missing": keys_missing,
            "loss_rate": report.get("loss_rate", 0.0),
            "verify_errors": report.get("verify_errors", 0),
            "duration_actual": report.get("duration_actual", 0),
            "write_rate_mean": np.mean(acked_rate) if acked_rate else 0,
            "partition_errors": sum(failed_rate),
            "affected_rate_peak": max(affected_rate) if affected_rate else 0,
            "p95_latency_ms": report.get("p95_latency_ms", 0.0),
            "p99_latency_ms": report.get("p99_latency_ms", 0.0),
            "max_latency_ms": report.get("max_latency_ms", 0.0),
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
        ("Failed request rate", "failed_rate"),
        ("Slow writes", "total_slow"),
        ("Affected writes", "total_affected"),
        ("Affected request rate", "affected_rate"),
        ("Peak affected rate/s", "affected_rate_peak"),
        ("p95 latency (ms)", "p95_latency_ms"),
        ("p99 latency (ms)", "p99_latency_ms"),
        ("Partition errors", "partition_errors"),
        ("Write rate mean (ops/s)", "write_rate_mean"),
        ("Verify errors", "verify_errors"),
    ]

    print(f"\n{'Metric':<35} {'Mean':>12} {'Std':>12}")
    print("-" * 61)
    for label, col in metrics:
        mean = df[col].mean()
        std = df[col].std()
        if col.endswith("_rate") or col == "affected_rate_peak":
            print(f"{label:<35} {mean * 100:>11.2f}% {std * 100:>11.2f}%")
        else:
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
        attempted = [e.get("attempted", 0) for e in ts]
        failed = [e.get("failed", 0) for e in ts]
        slow = [e.get("slow", 0) for e in ts]
        affected = [e.get("affected", e.get("failed", 0) + e.get("slow", 0)) for e in ts]
        affected_pct = [
            (a / total * 100) if total > 0 else 0
            for a, total in zip(affected, attempted)
        ]
        p95 = [e.get("p95_latency_ms", 0.0) for e in ts]
        p99 = [e.get("p99_latency_ms", 0.0) for e in ts]

        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 8), sharex=True)

        ax1.plot(seconds, attempted, linewidth=0.7, color="#8172b3", label="Attempted writes/s")
        ax1.plot(seconds, acked, linewidth=0.7, color="#4c72b0", label="ACK'd writes/s")
        ax1.set_ylabel("Writes/sec")
        loss_info = f" | lost={row['keys_missing']}" if row["keys_missing"] > 0 else " | no loss"
        affected_info = f" | affected={row.get('affected_rate', 0) * 100:.2f}%"
        ax1.set_title(f"Consistency Run {i + 1}{loss_info}{affected_info}")
        ax1.legend(fontsize=8)

        has_affected = any(count > 0 for count in affected)
        partition_window = _failure_window(seconds, affected)
        if has_affected:
            fail_start = None
            first_affected_second = seconds[[k for k, v in enumerate(affected) if v > 0][0]]
            for j, count in enumerate(affected):
                if count > 0 and fail_start is None:
                    fail_start = seconds[j]
                elif count == 0 and fail_start is not None:
                    ax1.axvspan(fail_start, seconds[j - 1], alpha=0.15, color="red",
                                label="Affected requests" if fail_start == first_affected_second else None)
                    fail_start = None
            if fail_start is not None:
                ax1.axvspan(fail_start, seconds[-1], alpha=0.15, color="red")

        if partition_window is not None:
            mark_test_window(
                ax1,
                partition_window[0],
                partition_window[1],
                with_labels=True,
                start_label="Impact start",
                end_label="Impact end",
            )
        ax1.legend(fontsize=8)

        ax2.plot(seconds, p95, linewidth=0.7, color="#dd8452", label="p95")
        ax2.plot(seconds, p99, linewidth=0.7, color="#c44e52", label="p99")
        slow_threshold = row.get("slow_threshold_ms")
        if slow_threshold is not None and not pd.isna(slow_threshold):
            ax2.axhline(
                slow_threshold,
                linestyle="--",
                color="gray",
                linewidth=0.7,
                label="Slow threshold",
            )
        ax2.set_ylabel("Latency (ms)")
        ax2.legend(fontsize=8)
        if partition_window is not None:
            mark_test_window(ax2, partition_window[0], partition_window[1])

        ax3.bar(seconds, failed, width=1.0, color="#c44e52", alpha=0.7, label="Failed writes/s")
        ax3.bar(seconds, slow, bottom=failed, width=1.0, color="#dd8452", alpha=0.7, label="Slow writes/s")
        ax3_twin = ax3.twinx()
        ax3_twin.plot(seconds, affected_pct, linewidth=0.8, color="#55a868", label="Affected %")
        ax3.set_ylabel("Affected writes/sec")
        ax3_twin.set_ylabel("Affected requests (%)")
        ax3.set_xlabel("Time (s)")
        ax3.legend(fontsize=8, loc="upper left")
        ax3_twin.legend(fontsize=8, loc="upper right")
        if partition_window is not None:
            mark_test_window(ax3, partition_window[0], partition_window[1])

        fig.tight_layout()
        fig.savefig(out_dir / f"consistency_run_{i + 1}.png", dpi=150, bbox_inches="tight")
        plt.close(fig)


def _failure_window(seconds: List[int], failed: List[int]) -> Optional[Tuple[int, int]]:
    failed_seconds = [second for second, count in zip(seconds, failed) if count > 0]
    if not failed_seconds:
        return None

    return min(failed_seconds), max(failed_seconds)


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

    ax2.bar(x, results_df["affected_rate"] * 100, color="#dd8452",
            edgecolor="black", linewidth=0.5)
    ax2.set_xticks(x)
    ax2.set_xticklabels([f"Run {j + 1}" for j in range(len(results_df))])
    ax2.set_ylabel("Affected requests (%)")
    ax2.set_title("User-Visible Impact per Run")

    fig.tight_layout()
    fig.savefig(out_dir / "consistency_comparison.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

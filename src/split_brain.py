"""Analysis module for split-brain consistency tests.

Extends the client-partition consistency analysis with per-side
(minority/majority) breakdown of ACK'd writes and data loss.
"""

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


def analyse_split_brain_runs(
    json_dir: Path,
) -> Tuple[pd.DataFrame, List[List[Dict]]]:
    """Parse all split_brain_run_*.json files."""
    files = sorted(json_dir.glob("split_brain_run_*.json"))
    if not files:
        raise FileNotFoundError(f"No split_brain_run_*.json files in {json_dir}")

    rows: List[Dict[str, Any]] = []
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

        acked_minority = report.get("acked_minority", 0)
        acked_majority = report.get("acked_majority", 0)
        keys_missing = report.get("keys_missing", 0)
        keys_missing_minority = report.get("keys_missing_minority", 0)
        keys_missing_majority = report.get("keys_missing_majority", 0)

        acked_rate = []
        failed_rate = []
        affected_rate = []
        for entry in ts:
            acked_rate.append(entry.get("acked", 0))
            failed_rate.append(entry.get("failed", 0))
            attempted = entry.get("attempted", 0)
            affected = entry.get("affected", entry.get("failed", 0) + entry.get("slow", 0))
            affected_rate.append(affected / attempted if attempted > 0 else 0)

        timing = _load_timing(json_dir, f)

        rows.append({
            "file": f.name,
            "run_id": report.get("run_id", ""),
            "clients": report.get("clients", 1),
            "socket_timeout": report.get("socket_timeout", None),
            "slow_threshold_ms": report.get("slow_threshold_ms", None),
            "minority_pods": ",".join(report.get("minority_pods", [])),
            "minority_slot_count": report.get("minority_slot_count", 0),
            "majority_slot_count": report.get("majority_slot_count", 0),
            "total_attempted": total_attempted,
            "total_acked": total_acked,
            "acked_minority": acked_minority,
            "acked_majority": acked_majority,
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
            "keys_missing": keys_missing,
            "keys_missing_minority": keys_missing_minority,
            "keys_missing_majority": keys_missing_majority,
            "loss_rate": report.get("loss_rate", 0.0),
            "minority_loss_rate": report.get("minority_loss_rate", 0.0),
            "majority_loss_rate": report.get("majority_loss_rate", 0.0),
            "verify_errors": report.get("verify_errors", 0),
            "duration_actual": report.get("duration_actual", 0),
            "write_rate_mean": np.mean(acked_rate) if acked_rate else 0,
            "partition_errors": sum(failed_rate),
            "affected_rate_peak": max(affected_rate) if affected_rate else 0,
            "p95_latency_ms": report.get("p95_latency_ms", 0.0),
            "p99_latency_ms": report.get("p99_latency_ms", 0.0),
            "max_latency_ms": report.get("max_latency_ms", 0.0),
            "chaos_epoch_s": timing.get("chaos_epoch_s"),
        })

    return pd.DataFrame(rows), all_ts


def _load_timing(json_dir: Path, run_file: Path) -> Dict[str, Any]:
    idx = run_file.stem.replace("split_brain_run_", "")
    timing_path = json_dir / f"split_brain_timing_{idx}.json"
    if not timing_path.exists():
        return {}
    with timing_path.open() as f:
        return json.load(f)


def print_split_brain_summary(df: pd.DataFrame) -> None:
    print(f"\nSplit-brain runs analysed: {len(df)}")

    total_lost = int(df["keys_missing"].sum())
    total_acked = int(df["total_acked"].sum())
    runs_with_loss = int((df["keys_missing"] > 0).sum())

    print(f"Runs with data loss: {runs_with_loss}/{len(df)}")
    if total_acked > 0:
        print(f"Total keys lost:     {total_lost} / {total_acked} ACK'd "
              f"({total_lost / total_acked * 100:.4f}%)")

    minority_lost = int(df["keys_missing_minority"].sum())
    majority_lost = int(df["keys_missing_majority"].sum())
    minority_acked = int(df["acked_minority"].sum())
    majority_acked = int(df["acked_majority"].sum())

    print(f"\nPer-side breakdown:")
    print(f"  Minority ACK'd:  {minority_acked}  |  Lost: {minority_lost}"
          f"  ({minority_lost / minority_acked * 100:.4f}%)" if minority_acked > 0 else
          f"  Minority ACK'd:  {minority_acked}  |  Lost: {minority_lost}")
    print(f"  Majority ACK'd:  {majority_acked}  |  Lost: {majority_lost}"
          f"  ({majority_lost / majority_acked * 100:.4f}%)" if majority_acked > 0 else
          f"  Majority ACK'd:  {majority_acked}  |  Lost: {majority_lost}")

    metrics = [
        ("Keys missing (total, per run)", "keys_missing"),
        ("Keys missing (minority)", "keys_missing_minority"),
        ("Keys missing (majority)", "keys_missing_majority"),
        ("Loss rate (total)", "loss_rate"),
        ("Minority loss rate", "minority_loss_rate"),
        ("Majority loss rate", "majority_loss_rate"),
        ("Total ACK'd writes", "total_acked"),
        ("ACK'd minority", "acked_minority"),
        ("ACK'd majority", "acked_majority"),
        ("Total failed writes", "total_failed"),
        ("Affected request rate", "affected_rate"),
        ("p95 latency (ms)", "p95_latency_ms"),
        ("p99 latency (ms)", "p99_latency_ms"),
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


def save_split_brain_csv(df: pd.DataFrame, out_dir: Path) -> None:
    csv_path = out_dir / "split_brain_summary.csv"
    df.to_csv(csv_path, index=False)
    print(f"\nSplit-brain CSV saved to {csv_path}")


def plot_split_brain_timeseries(
    all_ts: List[List[Dict]],
    results_df: pd.DataFrame,
    out_dir: Path,
) -> None:
    """Plot write rate over time with minority/majority breakdown."""
    for i, (ts, (_, row)) in enumerate(zip(all_ts, results_df.iterrows())):
        if not ts:
            continue

        seconds = [e["second"] for e in ts]
        acked = [e["acked"] for e in ts]
        attempted = [e.get("attempted", 0) for e in ts]
        failed = [e.get("failed", 0) for e in ts]
        slow = [e.get("slow", 0) for e in ts]
        acked_minority = [e.get("acked_minority", 0) for e in ts]
        acked_majority = [e.get("acked_majority", 0) for e in ts]
        failed_minority = [e.get("failed_minority", 0) for e in ts]
        failed_majority = [e.get("failed_majority", 0) for e in ts]
        affected = [e.get("affected", e.get("failed", 0) + e.get("slow", 0)) for e in ts]
        affected_pct = [
            (a / total * 100) if total > 0 else 0
            for a, total in zip(affected, attempted)
        ]
        p95 = [e.get("p95_latency_ms", 0.0) for e in ts]
        p99 = [e.get("p99_latency_ms", 0.0) for e in ts]

        fig, axes = plt.subplots(4, 1, figsize=(14, 12), sharex=True)
        ax_ops, ax_sides, ax_latency, ax_errors = axes

        # Panel 1: total ops
        ax_ops.plot(seconds, attempted, linewidth=0.7, color="#8172b3", label="Attempted/s")
        ax_ops.plot(seconds, acked, linewidth=0.7, color="#4c72b0", label="ACK'd/s")
        ax_ops.set_ylabel("Writes/sec")
        loss_info = (f" | lost={row['keys_missing']}"
                     f" (min={row['keys_missing_minority']}, maj={row['keys_missing_majority']})"
                     if row["keys_missing"] > 0 else " | no loss")
        ax_ops.set_title(f"Split-Brain Run {i + 1}{loss_info}")
        ax_ops.legend(fontsize=8)

        partition_window = _failure_window(seconds, affected)
        if partition_window is not None:
            mark_test_window(ax_ops, partition_window[0], partition_window[1],
                             with_labels=True, start_label="Impact start",
                             end_label="Impact end")
            ax_ops.legend(fontsize=8)

        # Panel 2: minority vs majority ACK rate
        ax_sides.stackplot(seconds, acked_minority, acked_majority,
                           colors=["#c44e52", "#55a868"], alpha=0.7,
                           labels=["Minority ACK'd/s", "Majority ACK'd/s"])
        ax_sides.set_ylabel("ACK'd writes/sec")
        ax_sides.legend(fontsize=8, loc="upper right")
        if partition_window is not None:
            mark_test_window(ax_sides, partition_window[0], partition_window[1])

        # Panel 3: latency
        ax_latency.plot(seconds, p95, linewidth=0.7, color="#dd8452", label="p95")
        ax_latency.plot(seconds, p99, linewidth=0.7, color="#c44e52", label="p99")
        slow_threshold = row.get("slow_threshold_ms")
        if slow_threshold is not None and not pd.isna(slow_threshold):
            ax_latency.axhline(slow_threshold, linestyle="--", color="gray",
                               linewidth=0.7, label="Slow threshold")
        ax_latency.set_ylabel("Latency (ms)")
        ax_latency.legend(fontsize=8)
        if partition_window is not None:
            mark_test_window(ax_latency, partition_window[0], partition_window[1])

        # Panel 4: errors by side
        ax_errors.bar(seconds, failed_minority, width=1.0, color="#c44e52",
                      alpha=0.7, label="Failed (minority slots)")
        ax_errors.bar(seconds, failed_majority, bottom=failed_minority,
                      width=1.0, color="#dd8452", alpha=0.7,
                      label="Failed (majority slots)")
        ax_err_twin = ax_errors.twinx()
        ax_err_twin.plot(seconds, affected_pct, linewidth=0.8, color="#55a868",
                         label="Affected %")
        ax_errors.set_ylabel("Failed writes/sec")
        ax_err_twin.set_ylabel("Affected requests (%)")
        ax_errors.set_xlabel("Time (s)")
        ax_errors.legend(fontsize=8, loc="upper left")
        ax_err_twin.legend(fontsize=8, loc="upper right")
        if partition_window is not None:
            mark_test_window(ax_errors, partition_window[0], partition_window[1])

        fig.tight_layout()
        fig.savefig(out_dir / f"split_brain_run_{i + 1}.png", dpi=150, bbox_inches="tight")
        plt.close(fig)


def _failure_window(seconds: List[int], affected: List[int]) -> Optional[Tuple[int, int]]:
    affected_seconds = [s for s, c in zip(seconds, affected) if c > 0]
    if not affected_seconds:
        return None
    return min(affected_seconds), max(affected_seconds)


def plot_split_brain_comparison(results_df: pd.DataFrame, out_dir: Path) -> None:
    """Bar charts comparing data loss across runs with minority/majority breakdown."""
    if results_df.empty:
        return

    n = len(results_df)
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    ax_loss, ax_rate, ax_affected = axes
    x = np.arange(n)

    # Panel 1: keys lost (stacked minority/majority)
    ax_loss.bar(x, results_df["keys_missing_minority"], color="#c44e52",
                edgecolor="black", linewidth=0.5, label="Lost (minority slots)")
    ax_loss.bar(x, results_df["keys_missing_majority"],
                bottom=results_df["keys_missing_minority"],
                color="#dd8452", edgecolor="black", linewidth=0.5,
                label="Lost (majority slots)")
    ax_loss.set_xticks(x)
    ax_loss.set_xticklabels([f"Run {j + 1}" for j in range(n)])
    ax_loss.set_ylabel("Keys lost (ACK'd but missing)")
    ax_loss.set_title("Data Loss per Run")
    ax_loss.legend(fontsize=8)
    if results_df["keys_missing"].sum() > 0:
        mean_val = results_df["keys_missing"].mean()
        ax_loss.axhline(mean_val, linestyle="--", color="gray", linewidth=0.7,
                        label=f"Mean total: {mean_val:.1f}")
        ax_loss.legend(fontsize=8)

    # Panel 2: minority vs majority loss rate
    width = 0.35
    ax_rate.bar(x - width / 2, results_df["minority_loss_rate"] * 100, width,
                color="#c44e52", edgecolor="black", linewidth=0.5, label="Minority loss %")
    ax_rate.bar(x + width / 2, results_df["majority_loss_rate"] * 100, width,
                color="#55a868", edgecolor="black", linewidth=0.5, label="Majority loss %")
    ax_rate.set_xticks(x)
    ax_rate.set_xticklabels([f"Run {j + 1}" for j in range(n)])
    ax_rate.set_ylabel("Loss rate (%)")
    ax_rate.set_title("Loss Rate by Partition Side")
    ax_rate.legend(fontsize=8)

    # Panel 3: affected request rate
    ax_affected.bar(x, results_df["affected_rate"] * 100, color="#dd8452",
                    edgecolor="black", linewidth=0.5)
    ax_affected.set_xticks(x)
    ax_affected.set_xticklabels([f"Run {j + 1}" for j in range(n)])
    ax_affected.set_ylabel("Affected requests (%)")
    ax_affected.set_title("User-Visible Impact per Run")

    fig.tight_layout()
    fig.savefig(out_dir / "split_brain_comparison.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

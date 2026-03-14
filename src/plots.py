from pathlib import Path
from typing import Any, Dict

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker
import numpy as np
import pandas as pd

from src.models import METRIC_COLS, ratio_label

matplotlib.use("Agg")


def _save(fig: plt.Figure, out_dir: Path, name: str) -> None:
    fig.savefig(out_dir / f"{name}.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def plot_ops_sec_bars(agg: pd.DataFrame, out_dir: Path) -> None:
    """Bar chart of ops/sec (mean +/- std) grouped by vCPU, one figure per (ratio, payload)."""
    for ratio in sorted(agg["ratio"].unique()):
        for payload in sorted(agg[agg["ratio"] == ratio]["payload"].unique()):
            sub = agg[(agg["ratio"] == ratio) & (agg["payload"] == payload)].sort_values("cpu")
            fig, ax = plt.subplots(figsize=(6, 4))
            x = np.arange(len(sub))
            ax.bar(x, sub["ops_sec_mean"], yerr=sub["ops_sec_std"], capsize=4,
                   color="#4c72b0", edgecolor="black", linewidth=0.5)
            ax.set_xticks(x)
            ax.set_xticklabels(sub["cpu"].astype(str))
            ax.set_xlabel("vCPU")
            ax.set_ylabel("Ops/sec")
            ax.set_title(f"Ops/sec  |  {ratio_label(ratio)}  |  {payload} KB")
            ax.yaxis.set_major_formatter(matplotlib.ticker.EngFormatter())
            fig.tight_layout()
            _save(fig, out_dir, f"ops_sec_{ratio}_{payload}kb")


def plot_latency_bars(agg: pd.DataFrame, out_dir: Path) -> None:
    """Grouped bar chart of p50/p95/p99/p99.9 per vCPU, one figure per (ratio, payload)."""
    percentiles = ["p50", "p95", "p99", "p999"]
    labels = ["p50", "p95", "p99", "p99.9"]
    colors = ["#4c72b0", "#dd8452", "#55a868", "#c44e52"]

    for ratio in sorted(agg["ratio"].unique()):
        for payload in sorted(agg[agg["ratio"] == ratio]["payload"].unique()):
            sub = agg[(agg["ratio"] == ratio) & (agg["payload"] == payload)].sort_values("cpu")
            fig, ax = plt.subplots(figsize=(8, 4))
            n_groups = len(sub)
            n_bars = len(percentiles)
            width = 0.8 / n_bars
            x = np.arange(n_groups)

            for j, (p, lbl, col) in enumerate(zip(percentiles, labels, colors)):
                offset = (j - n_bars / 2 + 0.5) * width
                ax.bar(
                    x + offset,
                    sub[f"{p}_mean"],
                    width,
                    yerr=sub[f"{p}_std"],
                    label=lbl,
                    color=col,
                    edgecolor="black",
                    linewidth=0.5,
                    capsize=3,
                )

            ax.set_xticks(x)
            ax.set_xticklabels(sub["cpu"].astype(str))
            ax.set_xlabel("vCPU")
            ax.set_ylabel("Latency (ms)")
            ax.set_title(f"Latency distribution  |  {ratio_label(ratio)}  |  {payload} KB")
            ax.legend()
            fig.tight_layout()
            _save(fig, out_dir, f"latency_{ratio}_{payload}kb")


def plot_cpu_mem(agg: pd.DataFrame, out_dir: Path) -> None:
    """CPU utilization and memory usage vs vCPU (skip if all NaN)."""
    has_cpu = agg["cpu_util_mean"].notna().any()
    has_mem = agg["memory_usage_mean"].notna().any()
    if not has_cpu and not has_mem:
        return

    for ratio in sorted(agg["ratio"].unique()):
        for payload in sorted(agg[agg["ratio"] == ratio]["payload"].unique()):
            sub = agg[(agg["ratio"] == ratio) & (agg["payload"] == payload)].sort_values("cpu")

            if has_cpu:
                fig, ax = plt.subplots(figsize=(6, 4))
                x = np.arange(len(sub))
                ax.bar(x, sub["cpu_util_mean"], yerr=sub["cpu_util_std"], capsize=4,
                       color="#dd8452", edgecolor="black", linewidth=0.5)
                ax.set_xticks(x)
                ax.set_xticklabels(sub["cpu"].astype(str))
                ax.set_xlabel("vCPU")
                ax.set_ylabel("CPU cores used")
                ax.set_title(f"CPU utilization  |  {ratio_label(ratio)}  |  {payload} KB")
                fig.tight_layout()
                _save(fig, out_dir, f"cpu_util_{ratio}_{payload}kb")

            if has_mem:
                fig, ax = plt.subplots(figsize=(6, 4))
                x = np.arange(len(sub))
                mem_mb = sub["memory_usage_mean"] / (1024 * 1024)
                mem_mb_std = sub["memory_usage_std"] / (1024 * 1024)
                ax.bar(x, mem_mb, yerr=mem_mb_std, capsize=4,
                       color="#55a868", edgecolor="black", linewidth=0.5)
                ax.set_xticks(x)
                ax.set_xticklabels(sub["cpu"].astype(str))
                ax.set_xlabel("vCPU")
                ax.set_ylabel("Memory (MB)")
                ax.set_title(f"Memory usage  |  {ratio_label(ratio)}  |  {payload} KB")
                fig.tight_layout()
                _save(fig, out_dir, f"mem_usage_{ratio}_{payload}kb")


def plot_heatmap(agg: pd.DataFrame, out_dir: Path) -> None:
    """Heatmap of ops/sec across (payload x vCPU) for each ratio."""
    for ratio in sorted(agg["ratio"].unique()):
        sub = agg[agg["ratio"] == ratio]
        pivot = sub.pivot_table(index="payload", columns="cpu", values="ops_sec_mean")
        pivot = pivot.sort_index(ascending=True)

        fig, ax = plt.subplots(figsize=(6, 4))
        im = ax.imshow(pivot.values, aspect="auto", cmap="YlOrRd")
        ax.set_xticks(range(len(pivot.columns)))
        ax.set_xticklabels([str(c) for c in pivot.columns])
        ax.set_yticks(range(len(pivot.index)))
        ax.set_yticklabels([f"{p} KB" for p in pivot.index])
        ax.set_xlabel("vCPU")
        ax.set_ylabel("Payload")
        ax.set_title(f"Ops/sec heatmap  |  {ratio_label(ratio)}")

        for i in range(len(pivot.index)):
            for j in range(len(pivot.columns)):
                val = pivot.values[i, j]
                ax.text(j, i, f"{val:,.0f}", ha="center", va="center", fontsize=8,
                        color="white" if val > pivot.values.max() * 0.6 else "black")

        fig.colorbar(im, ax=ax, label="Ops/sec")
        fig.tight_layout()
        _save(fig, out_dir, f"heatmap_{ratio}")


def save_summary_csv(agg: pd.DataFrame, out_dir: Path) -> None:
    """Save and print a summary table with mean +/- std for every metric."""
    rows = []
    for _, r in agg.iterrows():
        row: Dict[str, Any] = {
            "cpu": int(r["cpu"]),
            "payload_kb": int(r["payload"]),
            "ratio": ratio_label(str(r["ratio"])),
            "n": int(r["n"]),
        }
        for m in METRIC_COLS:
            mean_val = r[f"{m}_mean"]
            std_val = r[f"{m}_std"]
            if pd.isna(mean_val):
                row[f"{m}_mean"] = ""
                row[f"{m}_std"] = ""
            else:
                row[f"{m}_mean"] = f"{mean_val:.4f}"
                row[f"{m}_std"] = f"{std_val:.4f}"
        rows.append(row)

    summary = pd.DataFrame(rows)
    csv_path = out_dir / "summary.csv"
    summary.to_csv(csv_path, index=False)
    print(f"\nSummary saved to {csv_path}")
    print(summary.to_string(index=False))

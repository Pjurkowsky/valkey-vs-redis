"""Analysis module for backup & restore tests."""

import json
from pathlib import Path
from typing import List, Tuple

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

matplotlib.use("Agg")


def analyse_backup_runs(json_dir: Path) -> pd.DataFrame:
    """Parse all backup_timing_*.json files."""
    files = sorted(json_dir.glob("backup_timing_*.json"))
    if not files:
        raise FileNotFoundError(f"No backup_timing_*.json files in {json_dir}")

    rows = []
    for f in files:
        with f.open() as fh:
            doc = json.load(fh)
        rows.append({
            "file": f.name,
            "run": doc.get("run"),
            "size_mb": doc.get("size_mb"),
            "seed_keys": doc.get("seed_keys"),
            "seed_duration_s": doc.get("seed_duration_s"),
            "save_duration_s": doc.get("save_duration_s"),
            "restore_duration_s": doc.get("restore_duration_s"),
            "integrity_ok": doc.get("integrity_ok"),
        })

    return pd.DataFrame(rows)


def print_backup_summary(df: pd.DataFrame) -> None:
    print(f"\nBackup/restore runs analysed: {len(df)}")

    sizes = sorted(df["size_mb"].unique())
    integrity_ok = df["integrity_ok"].sum()
    print(f"Data integrity verified: {int(integrity_ok)}/{len(df)} runs")
    print(f"Dataset sizes tested: {', '.join(str(s) + ' MB' for s in sizes)}")

    metrics = [
        ("Seed duration (s)", "seed_duration_s"),
        ("Save duration (s)", "save_duration_s"),
        ("Restore duration (s)", "restore_duration_s"),
        ("Keys seeded", "seed_keys"),
    ]

    for size in sizes:
        subset = df[df["size_mb"] == size]
        print(f"\n--- {size} MB per shard ---")
        print(f"{'Metric':<30} {'Mean':>12} {'Std':>12}")
        print("-" * 56)
        for label, col in metrics:
            col_data = subset[col].dropna()
            if col_data.empty:
                continue
            mean = col_data.mean()
            std = col_data.std()
            print(f"{label:<30} {mean:>12.2f} {std:>12.2f}")


def save_backup_csv(df: pd.DataFrame, out_dir: Path) -> None:
    csv_path = out_dir / "backup_restore_summary.csv"
    df.to_csv(csv_path, index=False)
    print(f"\nBackup CSV saved to {csv_path}")


def plot_backup_restore_bars(df: pd.DataFrame, out_dir: Path) -> None:
    """Grouped bar chart: save time vs restore time for each dataset size."""
    sizes = sorted(df["size_mb"].unique())
    if not sizes:
        return

    save_means = []
    save_stds = []
    restore_means = []
    restore_stds = []

    for size in sizes:
        subset = df[df["size_mb"] == size]
        save_means.append(subset["save_duration_s"].mean())
        save_stds.append(subset["save_duration_s"].std())
        restore_means.append(subset["restore_duration_s"].mean())
        restore_stds.append(subset["restore_duration_s"].std())

    x = np.arange(len(sizes))
    width = 0.35

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(x - width / 2, save_means, width, yerr=save_stds,
           label="BGSAVE", color="#55a868", edgecolor="black", linewidth=0.5,
           capsize=3)
    ax.bar(x + width / 2, restore_means, width, yerr=restore_stds,
           label="Restore", color="#4c72b0", edgecolor="black", linewidth=0.5,
           capsize=3)

    ax.set_xlabel("Dataset size (MB per shard)")
    ax.set_ylabel("Duration (s)")
    ax.set_title("Backup (BGSAVE) vs Restore Duration")
    ax.set_xticks(x)
    ax.set_xticklabels([f"{s} MB" for s in sizes])
    ax.legend()

    fig.tight_layout()
    fig.savefig(out_dir / "backup_restore_bars.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def plot_restore_by_size(df: pd.DataFrame, out_dir: Path) -> None:
    """Scatter + line plot of restore time vs dataset size."""
    sizes = sorted(df["size_mb"].unique())
    if len(sizes) < 2:
        return

    fig, ax = plt.subplots(figsize=(7, 4))

    for size in sizes:
        subset = df[df["size_mb"] == size]
        ax.scatter(
            [size] * len(subset), subset["restore_duration_s"],
            color="#4c72b0", alpha=0.6, s=40, zorder=3,
        )

    means = [df[df["size_mb"] == s]["restore_duration_s"].mean() for s in sizes]
    ax.plot(sizes, means, marker="o", color="#c44e52", linewidth=1.5,
            markersize=6, label="Mean", zorder=4)

    ax.set_xlabel("Dataset size (MB per shard)")
    ax.set_ylabel("Restore duration (s)")
    ax.set_title("Restore Time vs Dataset Size")
    ax.legend()

    fig.tight_layout()
    fig.savefig(out_dir / "restore_by_size.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

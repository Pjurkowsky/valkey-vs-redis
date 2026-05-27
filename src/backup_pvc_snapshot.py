"""Analysis module for PVC snapshot backup & Memorystore backup benchmark results."""

import json
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

matplotlib.use("Agg")


def _load_valkey_runs(vk_dir: Path) -> pd.DataFrame:
    """Load Valkey PVC snapshot benchmark timing from run_N/ subdirectories."""
    rows = []
    for run_dir in sorted(vk_dir.glob("run_*")):
        timing_file = run_dir / "pvc_snapshot_benchmark_timing.json"
        if not timing_file.exists():
            continue
        with timing_file.open() as f:
            doc = json.load(f)
        rows.append({
            "provider": "valkey_self_hosted",
            "run": doc["run"],
            "dataset_mb": doc["dataset_mb"],
            "seed_keys": doc["seed_keys"],
            "seed_duration_s": doc["seed_duration_s"],
            "backup_duration_s": doc["backup_duration_s"],
            "bgsave_duration_s": doc["bgsave_duration_s"],
            "scale_down_duration_s": doc["scale_down_duration_s"],
            "snapshot_create_duration_s": doc["snapshot_create_duration_s"],
            "restore_duration_s": doc["restore_duration_s"],
            "disk_create_duration_s": doc["disk_create_duration_s"],
            "pv_create_duration_s": doc["pv_create_duration_s"],
            "pod_recreate_duration_s": doc["pod_recreate_duration_s"],
            "cluster_recovery_after_pods_s": doc["cluster_recovery_after_pods_s"],
            "verify_duration_s": doc["verify_duration_s"],
            "verify_sample_size": doc["verify_sample_size"],
            "keys_missing": doc["keys_missing"],
            "verify_errors": doc["verify_errors"],
            "integrity_ok": doc["integrity_ok"],
        })
    return pd.DataFrame(rows)


def _load_memorystore_runs(ms_dir: Path) -> pd.DataFrame:
    """Load Memorystore backup timing files."""
    rows = []
    for f in sorted(ms_dir.glob("memorystore_backup_timing_*.json")):
        with f.open() as fh:
            doc = json.load(fh)
        rows.append({
            "provider": "memorystore",
            "run": doc["run"],
            "dataset_mb": doc["dataset_mb"],
            "seed_keys": doc["seed_keys"],
            "seed_duration_s": doc["seed_duration_s"],
            "backup_duration_s": doc["backup_duration_s"],
            "restore_duration_s": doc["restore_duration_s"],
            "verify_duration_s": doc["verify_duration_s"],
            "integrity_ok": doc["integrity_ok"],
        })
    return pd.DataFrame(rows)


def analyse_backup_snapshot_runs(base_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse Valkey PVC snapshot and Memorystore backup results.

    Returns (valkey_df, memorystore_df).
    """
    vk_dir = base_dir / "vk"
    ms_dir = base_dir / "ms"

    vk_df = _load_valkey_runs(vk_dir) if vk_dir.exists() else pd.DataFrame()
    ms_df = _load_memorystore_runs(ms_dir) if ms_dir.exists() else pd.DataFrame()

    if vk_df.empty and ms_df.empty:
        raise FileNotFoundError(f"No backup results found in {base_dir}")

    return vk_df, ms_df


def print_backup_snapshot_summary(vk_df: pd.DataFrame, ms_df: pd.DataFrame) -> None:
    print("\n" + "=" * 70)
    print("BACKUP / RESTORE — PVC SNAPSHOT BENCHMARK SUMMARY")
    print("=" * 70)

    if not vk_df.empty:
        n = len(vk_df)
        ok = vk_df["integrity_ok"].sum()
        print(f"\n--- Valkey self-hosted (PVC snapshots) ---")
        print(f"Runs: {n}, integrity OK: {int(ok)}/{n}")
        print(f"Dataset: {vk_df['dataset_mb'].iloc[0]} MB, keys: {vk_df['seed_keys'].iloc[0]:,}")
        print(f"\n{'Phase':<30} {'Mean [s]':>10} {'Std [s]':>10}")
        print("-" * 52)
        phases = [
            ("BGSAVE", "bgsave_duration_s"),
            ("Scale-down StatefulSet", "scale_down_duration_s"),
            ("Snapshot create", "snapshot_create_duration_s"),
            ("Total backup", "backup_duration_s"),
            ("Disk create from snapshot", "disk_create_duration_s"),
            ("PV/PVC create", "pv_create_duration_s"),
            ("Pod recreate", "pod_recreate_duration_s"),
            ("Cluster recovery", "cluster_recovery_after_pods_s"),
            ("Total restore", "restore_duration_s"),
            ("Verify (10% sample)", "verify_duration_s"),
        ]
        for label, col in phases:
            mean = vk_df[col].mean()
            std = vk_df[col].std()
            print(f"{label:<30} {mean:>10.1f} {std:>10.2f}")

    if not ms_df.empty:
        n = len(ms_df)
        ok = ms_df["integrity_ok"].sum()
        print(f"\n--- Memorystore (managed backup) ---")
        print(f"Runs: {n}, integrity OK: {int(ok)}/{n}")
        print(f"Dataset: {ms_df['dataset_mb'].iloc[0]} MB, keys: {ms_df['seed_keys'].iloc[0]:,}")
        print(f"\n{'Phase':<30} {'Mean [s]':>10} {'Std [s]':>10}")
        print("-" * 52)
        ms_phases = [
            ("Backup (managed)", "backup_duration_s"),
            ("Restore (managed)", "restore_duration_s"),
            ("Verify", "verify_duration_s"),
        ]
        for label, col in ms_phases:
            mean = ms_df[col].mean()
            std = ms_df[col].std()
            print(f"{label:<30} {mean:>10.1f} {std:>10.2f}")

    if not vk_df.empty and not ms_df.empty:
        print(f"\n--- Comparison ---")
        vk_backup = vk_df["backup_duration_s"].mean()
        ms_backup = ms_df["backup_duration_s"].mean()
        vk_restore = vk_df["restore_duration_s"].mean()
        ms_restore = ms_df["restore_duration_s"].mean()
        print(f"{'Metric':<30} {'Valkey':>10} {'Memorystore':>12} {'Ratio':>8}")
        print("-" * 62)
        print(f"{'Backup [s]':<30} {vk_backup:>10.1f} {ms_backup:>12.1f} {vk_backup/ms_backup:>8.2f}x")
        print(f"{'Restore [s]':<30} {vk_restore:>10.1f} {ms_restore:>12.1f} {vk_restore/ms_restore:>8.2f}x")
        print(f"{'Total (backup+restore) [s]':<30} {vk_backup+vk_restore:>10.1f} {ms_backup+ms_restore:>12.1f} {(vk_backup+vk_restore)/(ms_backup+ms_restore):>8.2f}x")


def save_backup_snapshot_csv(vk_df: pd.DataFrame, ms_df: pd.DataFrame, out_dir: Path) -> None:
    if not vk_df.empty:
        path = out_dir / "backup_valkey_pvc_snapshot.csv"
        vk_df.to_csv(path, index=False)
        print(f"Valkey CSV saved to {path}")
    if not ms_df.empty:
        path = out_dir / "backup_memorystore.csv"
        ms_df.to_csv(path, index=False)
        print(f"Memorystore CSV saved to {path}")


def plot_backup_comparison(vk_df: pd.DataFrame, ms_df: pd.DataFrame, out_dir: Path) -> None:
    """Grouped bar chart comparing backup and restore times."""
    if vk_df.empty or ms_df.empty:
        return

    labels = ["Backup", "Restore"]
    vk_means = [vk_df["backup_duration_s"].mean(), vk_df["restore_duration_s"].mean()]
    vk_stds = [vk_df["backup_duration_s"].std(), vk_df["restore_duration_s"].std()]
    ms_means = [ms_df["backup_duration_s"].mean(), ms_df["restore_duration_s"].mean()]
    ms_stds = [ms_df["backup_duration_s"].std(), ms_df["restore_duration_s"].std()]

    x = np.arange(len(labels))
    width = 0.35

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.bar(x - width / 2, vk_means, width, yerr=vk_stds,
           label="Valkey (PVC snapshot)", color="#4c72b0", edgecolor="black",
           linewidth=0.5, capsize=4)
    ax.bar(x + width / 2, ms_means, width, yerr=ms_stds,
           label="Memorystore (managed)", color="#55a868", edgecolor="black",
           linewidth=0.5, capsize=4)

    ax.set_ylabel("Duration (s)")
    ax.set_title("Backup & Restore Duration — Valkey vs Memorystore")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()

    fig.tight_layout()
    fig.savefig(out_dir / "backup_comparison.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def plot_valkey_phase_breakdown(vk_df: pd.DataFrame, out_dir: Path) -> None:
    """Stacked bar chart of Valkey backup and restore phases."""
    if vk_df.empty:
        return

    backup_phases = [
        ("BGSAVE", "bgsave_duration_s", "#4c72b0"),
        ("Scale-down", "scale_down_duration_s", "#55a868"),
        ("Snapshot create", "snapshot_create_duration_s", "#c44e52"),
    ]
    restore_phases = [
        ("Disk create", "disk_create_duration_s", "#8172b2"),
        ("PV/PVC create", "pv_create_duration_s", "#ccb974"),
        ("Pod recreate", "pod_recreate_duration_s", "#64b5cd"),
        ("Cluster recovery", "cluster_recovery_after_pods_s", "#dd8452"),
    ]

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    # Backup
    ax = axes[0]
    bottom = 0
    for label, col, color in backup_phases:
        mean = vk_df[col].mean()
        ax.bar("Backup", mean, bottom=bottom, color=color, edgecolor="black",
               linewidth=0.5, label=f"{label} ({mean:.0f}s)")
        bottom += mean
    ax.set_ylabel("Duration (s)")
    ax.set_title("Valkey Backup — Phase Breakdown")
    ax.legend(loc="upper left")

    # Restore
    ax = axes[1]
    bottom = 0
    for label, col, color in restore_phases:
        mean = vk_df[col].mean()
        ax.bar("Restore", mean, bottom=bottom, color=color, edgecolor="black",
               linewidth=0.5, label=f"{label} ({mean:.0f}s)")
        bottom += mean
    ax.set_ylabel("Duration (s)")
    ax.set_title("Valkey Restore — Phase Breakdown")
    ax.legend(loc="upper left")

    fig.tight_layout()
    fig.savefig(out_dir / "valkey_backup_phase_breakdown.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

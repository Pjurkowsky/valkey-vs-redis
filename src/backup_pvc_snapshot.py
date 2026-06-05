"""Analysis module for online PVC snapshot and Memorystore backup results."""

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


SELF_HOSTED_PROVIDERS = {
    "vk": ("valkey_self_hosted", "Valkey"),
    "redis": ("redis72_self_hosted", "Redis 7.2"),
}

PHASES = [
    "backup_duration_s",
    "restore_duration_s",
    "seed_duration_s",
    "verify_duration_s",
]

SELF_HOSTED_BACKUP_PHASES = [
    ("BGSAVE", "bgsave_duration_s", "#4c72b0"),
    ("RDB validation", "rdb_validation_duration_s", "#55a868"),
    ("Snapshot create", "snapshot_create_duration_s", "#c44e52"),
]

SELF_HOSTED_RESTORE_PHASES = [
    ("Fresh cluster", "fresh_cluster_create_duration_s", "#8172b2"),
    ("Source disk create", "source_disk_create_duration_s", "#ccb974"),
    ("RDB copy", "rdb_incluster_copy_duration_s", "#64b5cd"),
    ("Replica clear", "replica_clear_duration_s", "#dd8452"),
    ("Temp cleanup", "temp_source_cleanup_duration_s", "#937860"),
    ("Pod recreate", "pod_recreate_duration_s", "#da8bc3"),
    ("Cluster recovery", "cluster_recovery_after_pods_s", "#8c8c8c"),
]


def _load_online_snapshot_summary(provider_dir: Path, provider: str, label: str) -> pd.DataFrame:
    summary = provider_dir / "online_replica_snapshot_summary.csv"
    if not summary.exists():
        return pd.DataFrame()

    df = pd.read_csv(summary)
    df["provider"] = provider
    df["provider_label"] = label
    df["dataset_mb"] = df["dataset_mb_total"]
    df["seed_keys"] = df["expected_keys"]
    return df


def _load_memorystore_runs(ms_dir: Path) -> pd.DataFrame:
    rows = []
    for f in sorted(ms_dir.glob("memorystore_backup_timing_*.json")):
        with f.open() as fh:
            doc = json.load(fh)
        rows.append({
            "provider": "memorystore",
            "provider_label": "Memorystore",
            "run": doc["run"],
            "dataset_mb": doc["dataset_mb"],
            "seed_keys": doc["seed_keys"],
            "source_flush_duration_s": doc.get("source_flush_duration_s"),
            "seed_duration_s": doc.get("seed_duration_s"),
            "seed_wall_duration_s": doc.get("seed_wall_duration_s"),
            "backup_duration_s": doc["backup_duration_s"],
            "restore_duration_s": doc["restore_duration_s"],
            "verify_duration_s": doc["verify_duration_s"],
            "verify_mode": doc.get("verify_mode"),
            "restored_keys": doc.get("restored_keys"),
            "key_count_ok": doc.get("key_count_ok"),
            "used_memory_dataset": doc.get("used_memory_dataset"),
            "integrity_ok": doc["integrity_ok"],
        })
    return pd.DataFrame(rows)


def analyse_backup_snapshot_runs(base_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse self-hosted online PVC snapshot and Memorystore backup results."""
    self_hosted = []
    for dirname, (provider, label) in SELF_HOSTED_PROVIDERS.items():
        provider_df = _load_online_snapshot_summary(base_dir / dirname, provider, label)
        if not provider_df.empty:
            self_hosted.append(provider_df)

    self_hosted_df = (
        pd.concat(self_hosted, ignore_index=True)
        if self_hosted else pd.DataFrame()
    )
    ms_df = _load_memorystore_runs(base_dir / "ms") if (base_dir / "ms").exists() else pd.DataFrame()

    if self_hosted_df.empty and ms_df.empty:
        raise FileNotFoundError(f"No backup results found in {base_dir}")

    return self_hosted_df, ms_df


def _stats(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    rows = []
    for provider, group in df.groupby("provider_label", sort=False):
        row = {
            "provider": provider,
            "runs": len(group),
            "integrity_ok": int(group["integrity_ok"].sum()),
            "dataset_mb": group["dataset_mb"].iloc[0],
            "seed_keys": group["seed_keys"].iloc[0],
        }
        for col in columns:
            if col in group:
                row[f"{col}_mean"] = group[col].mean()
                row[f"{col}_std"] = group[col].std()
        rows.append(row)
    return pd.DataFrame(rows)


def print_backup_snapshot_summary(self_hosted_df: pd.DataFrame, ms_df: pd.DataFrame) -> None:
    print("\n" + "=" * 78)
    print("BACKUP / RESTORE - ONLINE PVC SNAPSHOT AND MANAGED BACKUP SUMMARY")
    print("=" * 78)

    if not self_hosted_df.empty:
        for label, group in self_hosted_df.groupby("provider_label", sort=False):
            n = len(group)
            ok = int(group["integrity_ok"].sum())
            print(f"\n--- {label} self-hosted (online replica PVC snapshots) ---")
            print(f"Runs: {n}, integrity OK: {ok}/{n}")
            print(f"Dataset: {group['dataset_mb'].iloc[0]} MB, keys: {group['seed_keys'].iloc[0]:,}")
            print(f"\n{'Phase':<34} {'Mean [s]':>10} {'Std [s]':>10}")
            print("-" * 58)
            for label_name, col in [
                ("BGSAVE on replicas", "bgsave_duration_s"),
                ("RDB validation", "rdb_validation_duration_s"),
                ("Snapshot create", "snapshot_create_duration_s"),
                ("Total backup", "backup_duration_s"),
                ("Fresh cluster create", "fresh_cluster_create_duration_s"),
                ("Source disk create", "source_disk_create_duration_s"),
                ("RDB copy in cluster", "rdb_incluster_copy_duration_s"),
                ("Replica PVC clear", "replica_clear_duration_s"),
                ("Pod recreate", "pod_recreate_duration_s"),
                ("Cluster recovery", "cluster_recovery_after_pods_s"),
                ("Total restore", "restore_duration_s"),
                ("Verify", "verify_duration_s"),
            ]:
                mean = group[col].mean()
                std = group[col].std()
                print(f"{label_name:<34} {mean:>10.1f} {std:>10.2f}")

    if not ms_df.empty:
        n = len(ms_df)
        ok = int(ms_df["integrity_ok"].sum())
        print(f"\n--- Memorystore (managed backup) ---")
        print(f"Runs: {n}, integrity OK: {ok}/{n}")
        print(f"Dataset: {ms_df['dataset_mb'].iloc[0]} MB, keys: {ms_df['seed_keys'].iloc[0]:,}")
        print(f"\n{'Phase':<34} {'Mean [s]':>10} {'Std [s]':>10}")
        print("-" * 58)
        for label_name, col in [
            ("Backup (managed)", "backup_duration_s"),
            ("Restore (managed)", "restore_duration_s"),
            ("Verify", "verify_duration_s"),
        ]:
            mean = ms_df[col].mean()
            std = ms_df[col].std()
            print(f"{label_name:<34} {mean:>10.1f} {std:>10.2f}")

    combined = pd.concat([self_hosted_df, ms_df], ignore_index=True)
    if not combined.empty:
        print(f"\n--- Comparison ---")
        print(f"{'Provider':<16} {'Backup [s]':>12} {'Restore [s]':>12} {'Total [s]':>12}")
        print("-" * 58)
        for provider, group in combined.groupby("provider_label", sort=False):
            backup = group["backup_duration_s"].mean()
            restore = group["restore_duration_s"].mean()
            print(f"{provider:<16} {backup:>12.1f} {restore:>12.1f} {backup + restore:>12.1f}")


def save_backup_snapshot_csv(self_hosted_df: pd.DataFrame, ms_df: pd.DataFrame, out_dir: Path) -> None:
    if not self_hosted_df.empty:
        path = out_dir / "backup_self_hosted_online_snapshot.csv"
        self_hosted_df.to_csv(path, index=False)
        print(f"Self-hosted CSV saved to {path}")
    if not ms_df.empty:
        path = out_dir / "backup_memorystore.csv"
        ms_df.to_csv(path, index=False)
        print(f"Memorystore CSV saved to {path}")

    combined = pd.concat([self_hosted_df, ms_df], ignore_index=True)
    if not combined.empty:
        summary = _stats(combined, PHASES)
        summary_path = out_dir / "backup_restore_provider_summary.csv"
        summary.to_csv(summary_path, index=False)
        print(f"Provider summary CSV saved to {summary_path}")


def _provider_order(df: pd.DataFrame) -> list[str]:
    order = ["Valkey", "Redis 7.2", "Memorystore"]
    labels = list(df["provider_label"].drop_duplicates())
    return [label for label in order if label in labels] + [label for label in labels if label not in order]


def plot_backup_comparison(self_hosted_df: pd.DataFrame, ms_df: pd.DataFrame, out_dir: Path) -> None:
    """Grouped bar chart comparing backup and restore times."""
    combined = pd.concat([self_hosted_df, ms_df], ignore_index=True)
    if combined.empty:
        return

    providers = _provider_order(combined)
    labels = ["Backup", "Restore"]
    x = np.arange(len(labels))
    width = 0.22 if len(providers) >= 3 else 0.32
    offsets = np.linspace(-(len(providers) - 1) / 2, (len(providers) - 1) / 2, len(providers)) * width
    colors = {
        "Valkey": "#4c72b0",
        "Redis 7.2": "#c44e52",
        "Memorystore": "#55a868",
    }

    fig, ax = plt.subplots(figsize=(8, 5))
    max_label_height = 0.0
    for offset, provider in zip(offsets, providers):
        group = combined[combined["provider_label"] == provider]
        means = [group["backup_duration_s"].mean(), group["restore_duration_s"].mean()]
        stds = [group["backup_duration_s"].std(), group["restore_duration_s"].std()]
        bars = ax.bar(
            x + offset,
            means,
            width,
            yerr=stds,
            label=provider,
            color=colors.get(provider, "#8c8c8c"),
            edgecolor="black",
            linewidth=0.5,
            capsize=4,
        )
        for bar, mean, std in zip(bars, means, stds):
            label_height = mean + (0 if pd.isna(std) else std)
            max_label_height = max(max_label_height, label_height)
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                label_height + 8,
                f"{mean:.0f}s",
                ha="center",
                va="bottom",
                fontsize=8,
                fontweight="bold",
            )

    ax.set_ylabel("Duration (s)")
    ax.set_title("Backup and restore duration")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()
    ax.grid(axis="y", alpha=0.25)
    if max_label_height > 0:
        ax.set_ylim(0, max_label_height * 1.16)

    fig.tight_layout()
    fig.savefig(out_dir / "backup_comparison.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def plot_self_hosted_phase_breakdown(self_hosted_df: pd.DataFrame, out_dir: Path) -> None:
    """Stacked bar chart of self-hosted backup and restore phases."""
    if self_hosted_df.empty:
        return

    providers = _provider_order(self_hosted_df)
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    for ax, phases, title, total_col in [
        (axes[0], SELF_HOSTED_BACKUP_PHASES, "Backup phases", "backup_duration_s"),
        (axes[1], SELF_HOSTED_RESTORE_PHASES, "Restore phases", "restore_duration_s"),
    ]:
        x = np.arange(len(providers))
        bottoms = np.zeros(len(providers))
        for label, col, color in phases:
            values = []
            for provider in providers:
                group = self_hosted_df[self_hosted_df["provider_label"] == provider]
                values.append(group[col].mean())
            bars = ax.bar(
                x,
                values,
                bottom=bottoms,
                color=color,
                edgecolor="black",
                linewidth=0.5,
                label=label,
            )
            for bar, value, start in zip(bars, values, bottoms):
                if value >= 8:
                    ax.text(
                        bar.get_x() + bar.get_width() / 2,
                        start + value / 2,
                        f"{value:.0f}s",
                        ha="center",
                        va="center",
                        fontsize=8,
                    )
            bottoms += np.array(values)

        totals = []
        for provider in providers:
            group = self_hosted_df[self_hosted_df["provider_label"] == provider]
            totals.append(group[total_col].mean())
        totals = np.array(totals)

        residuals = np.maximum(totals - bottoms, 0)
        if np.any(residuals >= 1):
            bars = ax.bar(
                x,
                residuals,
                bottom=bottoms,
                color="#bab0ac",
                edgecolor="black",
                linewidth=0.5,
                label="Other/wait",
            )
            for bar, value, start in zip(bars, residuals, bottoms):
                if value >= 8:
                    ax.text(
                        bar.get_x() + bar.get_width() / 2,
                        start + value / 2,
                        f"{value:.0f}s",
                        ha="center",
                        va="center",
                        fontsize=8,
                    )

        label_heights = np.maximum(totals, bottoms + residuals)
        label_offset = max(label_heights.max() * 0.02, 1)
        for pos, total, label_height in zip(x, totals, label_heights):
            ax.text(
                pos,
                label_height + label_offset,
                f"{total:.0f}s",
                ha="center",
                va="bottom",
                fontsize=8,
                fontweight="bold",
            )
        ax.set_title(title)
        ax.set_ylabel("Duration (s)")
        ax.set_xticks(x)
        ax.set_xticklabels(providers)
        ax.grid(axis="y", alpha=0.25)
        ax.set_ylim(0, max(label_heights.max() * 1.16, label_heights.max() + 5))
        if len(phases) > 4:
            ax.legend(
                fontsize=8,
                loc="upper left",
                bbox_to_anchor=(1.02, 1),
                borderaxespad=0,
                frameon=False,
            )
        else:
            ax.legend(
                fontsize=8,
                loc="upper center",
                bbox_to_anchor=(0.5, -0.13),
                ncol=2,
                frameon=False,
            )

    fig.tight_layout()
    fig.savefig(out_dir / "backup_phase_breakdown.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

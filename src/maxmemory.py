"""Analysis module for maxmemory eviction tests."""

import json
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

matplotlib.use("Agg")


def analyse_maxmemory_runs(json_dir: Path) -> pd.DataFrame:
    """Parse all maxmemory_summary_*.json files."""
    files = sorted(json_dir.glob("maxmemory_summary_*.json"))
    if not files:
        raise FileNotFoundError(f"No maxmemory_summary_*.json files in {json_dir}")

    rows = []
    for f in files:
        with f.open() as fh:
            doc = json.load(fh)
        rows.append({
            "file": f.name,
            "run": doc.get("run"),
            "provider": doc.get("provider", "valkey"),
            "policy": doc.get("policy", "unknown"),
            "observed_policy": doc.get("observed_policy", doc.get("policy", "unknown")),
            "run_id": doc.get("run_id"),
            "target_mb": doc.get("target_mb"),
            "ttl_seconds": doc.get("ttl_seconds", 0),
            "seed_duration_s": doc.get("seed_duration_s"),
            "seed_wall_duration_s": doc.get("seed_wall_duration_s"),
            "target_keys": doc.get("target_keys"),
            "written_keys": doc.get("written_keys"),
            "seed_completed": doc.get("seed_completed", True),
            "pipeline_errors": doc.get("pipeline_errors", doc.get("errors", 0)),
            "write_errors": doc.get("write_errors", 0),
            "oom_errors": doc.get("oom_errors", 0),
            "error_replies_delta": doc.get("error_replies_delta", 0),
            "used_memory_before_mb": _bytes_to_mb(doc.get("used_memory_before", 0)),
            "used_memory_after_mb": _bytes_to_mb(doc.get("used_memory_after", 0)),
            "maxmemory_total_mb": _bytes_to_mb(doc.get("maxmemory_total_bytes", 0)),
            "dbsize_before": doc.get("dbsize_before"),
            "dbsize_after": doc.get("dbsize_after"),
            "evicted_keys_delta": doc.get("evicted_keys_delta"),
            "sample_size": doc.get("sample_size"),
            "sample_missing": doc.get("sample_missing"),
            "sample_missing_rate": doc.get("sample_missing_rate"),
            "verify_errors": doc.get("verify_errors"),
        })

    return pd.DataFrame(rows)


def print_maxmemory_summary(df: pd.DataFrame) -> None:
    print(f"\nMaxmemory runs analysed: {len(df)}")
    print(f"Targets tested: {', '.join(str(v) + ' MB' for v in sorted(df['target_mb'].unique()))}")
    print(f"Policies tested: {', '.join(str(v) for v in sorted(df['policy'].unique()))}")

    metrics = [
        ("Written keys", "written_keys"),
        ("Evicted keys", "evicted_keys_delta"),
        ("OOM errors", "oom_errors"),
        ("Write errors", "write_errors"),
        ("Error replies", "error_replies_delta"),
        ("Sample missing rate", "sample_missing_rate"),
        ("Used memory after (MB)", "used_memory_after_mb"),
        ("DB size after", "dbsize_after"),
        ("Seed duration (s)", "seed_duration_s"),
    ]

    for (provider, policy), group in df.groupby(["provider", "policy"], dropna=False):
        print(f"\n--- {provider} / {policy} ---")
        print(f"{'Metric':<28} {'Mean':>12} {'Std':>12}")
        print("-" * 54)
        for label, col in metrics:
            mean = group[col].mean()
            std = group[col].std()
            print(f"{label:<28} {mean:>12.2f} {std:>12.2f}")


def save_maxmemory_csv(df: pd.DataFrame, out_dir: Path) -> None:
    csv_path = out_dir / "maxmemory_summary.csv"
    df.to_csv(csv_path, index=False)
    print(f"\nMaxmemory CSV saved to {csv_path}")


def plot_memory_before_after(df: pd.DataFrame, out_dir: Path) -> None:
    labels = _run_labels(df)
    x = np.arange(len(df))
    width = 0.35

    fig, ax = plt.subplots(figsize=(max(8, len(df) * 0.85), 4.5))
    ax.bar(
        x - width / 2, df["used_memory_before_mb"], width,
        label="Before", color="#55a868", edgecolor="black", linewidth=0.5,
    )
    ax.bar(
        x + width / 2, df["used_memory_after_mb"], width,
        label="After", color="#4c72b0", edgecolor="black", linewidth=0.5,
    )

    ax.set_xlabel("Run")
    ax.set_ylabel("Used memory across masters (MB)")
    ax.set_title("Maxmemory Test: Memory Before vs After Writes")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.tick_params(axis="x", rotation=30)
    ax.legend()

    fig.tight_layout()
    fig.savefig(out_dir / "maxmemory_memory_before_after.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def plot_evictions_and_missing(df: pd.DataFrame, out_dir: Path) -> None:
    labels = _run_labels(df)
    x = np.arange(len(df))

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(max(11, len(df) * 1.2), 4.5))

    ax1.bar(
        x, df["evicted_keys_delta"],
        color="#dd8452", edgecolor="black", linewidth=0.5,
    )
    ax1.set_xlabel("Run")
    ax1.set_ylabel("Evicted keys")
    ax1.set_title("Evictions Triggered")
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels)
    ax1.tick_params(axis="x", rotation=30)

    ax2.bar(
        x, df["oom_errors"],
        color="#c44e52", edgecolor="black", linewidth=0.5,
    )
    ax2.set_xlabel("Run")
    ax2.set_ylabel("OOM errors")
    ax2.set_title("Write Rejections")
    ax2.set_xticks(x)
    ax2.set_xticklabels(labels)
    ax2.tick_params(axis="x", rotation=30)
    ax2.set_ylim(0, max(5, float(df["oom_errors"].max()) * 1.2))

    fig.tight_layout()
    fig.savefig(out_dir / "maxmemory_evictions_missing.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def _bytes_to_mb(value: int | float | None) -> float:
    return float(value or 0) / 1024 / 1024


def _run_labels(df: pd.DataFrame) -> list[str]:
    labels = []
    for i, row in df.reset_index(drop=True).iterrows():
        run = int(row["run"]) if pd.notna(row["run"]) else i + 1
        provider = str(row.get("provider", ""))
        policy = str(row.get("policy", ""))
        if provider and policy and policy != "unknown":
            labels.append(f"{provider[:2]} {policy} r{run}")
        else:
            labels.append(f"Run {run}")
    return labels

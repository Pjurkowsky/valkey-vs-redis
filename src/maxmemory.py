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
            "benchmark": doc.get("benchmark", "maxmemory"),
            "variant": doc.get("variant"),
            "run": doc.get("run"),
            "provider": doc.get("provider", "valkey"),
            "policy": doc.get("policy", "unknown"),
            "observed_policy": doc.get("observed_policy", doc.get("policy", "unknown")),
            "run_id": doc.get("run_id"),
            "target_mb": doc.get("target_mb"),
            "maxmemory_reference_mb": doc.get("maxmemory_reference_mb"),
            "ttl_seconds": doc.get("ttl_seconds", 0),
            "test_time_s": doc.get("test_time_s"),
            "observe_duration_s": doc.get("observe_duration_s"),
            "fill_to_observe_gap_s": doc.get("fill_to_observe_gap_s"),
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
            "used_memory_after_fill_mb": _bytes_to_mb(doc.get("used_memory_after_fill", doc.get("used_memory_after", 0))),
            "used_memory_after_observe_mb": _bytes_to_mb(doc.get("used_memory_after_observe", doc.get("used_memory_after", 0))),
            "used_memory_after_mb": _bytes_to_mb(doc.get("used_memory_after", 0)),
            "maxmemory_total_mb": _bytes_to_mb(doc.get("maxmemory_total_bytes", 0)),
            "dbsize_before": doc.get("dbsize_before"),
            "dbsize_after_fill": doc.get("dbsize_after_fill"),
            "dbsize_after_observe": doc.get("dbsize_after_observe"),
            "dbsize_after": doc.get("dbsize_after"),
            "evicted_keys_delta_prefill": doc.get("evicted_keys_delta_prefill"),
            "evicted_keys_delta_observe": doc.get("evicted_keys_delta_observe"),
            "evicted_keys_delta_total": doc.get("evicted_keys_delta_total", doc.get("evicted_keys_delta")),
            "evicted_keys_delta": doc.get("evicted_keys_delta"),
            "error_replies_delta_prefill": doc.get("error_replies_delta_prefill"),
            "error_replies_delta_observe": doc.get("error_replies_delta_observe"),
            "sample_size": doc.get("sample_size"),
            "sample_missing": doc.get("sample_missing"),
            "sample_missing_rate": doc.get("sample_missing_rate"),
            "verify_errors": doc.get("verify_errors"),
            "prefill_memtier_ops_sec": doc.get("prefill_memtier_ops_sec"),
            "prefill_memtier_count": doc.get("prefill_memtier_count"),
            "prefill_memtier_connection_errors": doc.get("prefill_memtier_connection_errors"),
            "prefill_memtier_avg_latency_ms": doc.get("prefill_memtier_avg_latency_ms"),
            "prefill_memtier_max_latency_ms": doc.get("prefill_memtier_max_latency_ms"),
            "prefill_memtier_p50_ms": doc.get("prefill_memtier_p50_ms"),
            "prefill_memtier_p95_ms": doc.get("prefill_memtier_p95_ms"),
            "prefill_memtier_p99_ms": doc.get("prefill_memtier_p99_ms"),
            "prefill_memtier_p999_ms": doc.get("prefill_memtier_p999_ms"),
            "memtier_ops_sec": doc.get("memtier_ops_sec"),
            "memtier_count": doc.get("memtier_count"),
            "memtier_connection_errors": doc.get("memtier_connection_errors"),
            "memtier_avg_latency_ms": doc.get("memtier_avg_latency_ms"),
            "memtier_max_latency_ms": doc.get("memtier_max_latency_ms"),
            "memtier_p50_ms": doc.get("memtier_p50_ms"),
            "memtier_p95_ms": doc.get("memtier_p95_ms"),
            "memtier_p99_ms": doc.get("memtier_p99_ms"),
            "memtier_p999_ms": doc.get("memtier_p999_ms"),
            "prefill_report_status": doc.get("prefill_report_status", doc.get("maxmemory_monitor_status")),
            "prefill_sample_count": doc.get("prefill_sample_count", doc.get("maxmemory_monitor_sample_count")),
            "prefill_dbsize_growth_ops_sec_to_100": doc.get("prefill_dbsize_growth_ops_sec_to_100"),
            "prefill_last_dbsize_growth_ops_sec_to_100": doc.get("prefill_last_dbsize_growth_ops_sec_to_100"),
            "prefill_evictions_sec_to_100": doc.get("prefill_evictions_sec_to_100"),
            "prefill_error_replies_sec_to_100": doc.get("prefill_error_replies_sec_to_100"),
            "maxmemory_reached": doc.get("maxmemory_reached"),
            "maxmemory_reached_elapsed_s": doc.get("maxmemory_reached_elapsed_s"),
            "maxmemory_reached_used_memory_pct": doc.get("maxmemory_reached_used_memory_pct"),
            "prefill_last_used_memory_pct": doc.get("prefill_last_used_memory_pct", doc.get("maxmemory_monitor_last_used_memory_pct")),
            "prefill_reached_dbsize": doc.get("prefill_reached_dbsize"),
            "prefill_reached_evicted_keys": doc.get("prefill_reached_evicted_keys"),
            "prefill_reached_error_replies": doc.get("prefill_reached_error_replies"),
            "prefill_last_dbsize": doc.get("prefill_last_dbsize"),
            "prefill_last_evicted_keys": doc.get("prefill_last_evicted_keys"),
            "prefill_last_error_replies": doc.get("prefill_last_error_replies"),
        })

    df = pd.DataFrame(rows)
    if "evicted_keys_delta" in df.columns and "evicted_keys_delta_total" in df.columns:
        df["evicted_keys_delta"] = df["evicted_keys_delta"].combine_first(df["evicted_keys_delta_total"])

    zero_fill_columns = [
        "evicted_keys_delta",
        "evicted_keys_delta_prefill",
        "evicted_keys_delta_observe",
        "evicted_keys_delta_total",
        "oom_errors",
        "write_errors",
        "pipeline_errors",
        "error_replies_delta",
        "error_replies_delta_prefill",
        "error_replies_delta_observe",
        "prefill_memtier_connection_errors",
        "memtier_connection_errors",
    ]
    for col in zero_fill_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


def print_maxmemory_summary(df: pd.DataFrame) -> None:
    print(f"\nMaxmemory runs analysed: {len(df)}")
    print(f"Targets tested: {', '.join(str(v) + ' MB' for v in sorted(df['target_mb'].unique()))}")
    print(f"Policies tested: {', '.join(str(v) for v in sorted(df['policy'].unique()))}")

    metrics = [
        ("Written keys", "written_keys"),
        ("Prefill memtier ops/sec", "prefill_memtier_ops_sec"),
        ("Prefill memtier p99 (ms)", "prefill_memtier_p99_ms"),
        ("Prefill memtier conn errors", "prefill_memtier_connection_errors"),
        ("Prefill samples", "prefill_sample_count"),
        ("Prefill ops/sec to 100%", "prefill_dbsize_growth_ops_sec_to_100"),
        ("Last prefill ops/sec", "prefill_last_dbsize_growth_ops_sec_to_100"),
        ("Time to maxmemory (s)", "maxmemory_reached_elapsed_s"),
        ("Memory at reach (%)", "maxmemory_reached_used_memory_pct"),
        ("DB size at reach", "prefill_reached_dbsize"),
        ("Evicted at reach", "prefill_reached_evicted_keys"),
        ("Evicted keys", "evicted_keys_delta"),
        ("Evicted during prefill", "evicted_keys_delta_prefill"),
        ("Evicted during observe", "evicted_keys_delta_observe"),
        ("OOM errors", "oom_errors"),
        ("Write errors", "write_errors"),
        ("Error replies", "error_replies_delta"),
        ("Memtier ops/sec", "memtier_ops_sec"),
        ("Memtier p99 (ms)", "memtier_p99_ms"),
        ("Memtier conn errors", "memtier_connection_errors"),
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

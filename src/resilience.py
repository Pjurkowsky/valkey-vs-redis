import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.chart_markers import mark_test_window
from src.failover import parse_time_series

matplotlib.use("Agg")

BASELINE_SECONDS = 25
DEGRADATION_THRESHOLD = 0.80
RECOVERY_THRESHOLD = 0.90
STRESS_START_SECOND = 30
STRESS_DURATIONS = {
    "cpu": 30,
    "memory": 60,
    "memory-extreme": 60,
    "maxmemory": 60,
}


def detect_degradation(ts: pd.DataFrame) -> Dict[str, Any]:
    """Analyse a time series for performance degradation under stress.

    Unlike failover detection (which looks for ops dropping to ~0), this looks
    for partial degradation and whether the system recovers after stress ends.
    """
    if ts.empty:
        return _empty_result()

    available_baseline = min(BASELINE_SECONDS, len(ts) // 2)
    if available_baseline < 3:
        return _empty_result()

    baseline = ts[ts["second"] < available_baseline]
    baseline_ops = baseline["count"].mean()
    baseline_p99 = baseline["p99"].mean()
    baseline_p999 = baseline["p999"].mean()

    if baseline_ops == 0:
        return _empty_result()

    ts = _trim_terminal_partial_bucket(ts, baseline_ops)

    threshold = baseline_ops * DEGRADATION_THRESHOLD
    degraded = ts[(ts["second"] >= available_baseline) & (ts["count"] < threshold)]

    if degraded.empty:
        return {
            "degradation_detected": False,
            "baseline_ops": baseline_ops,
            "baseline_p99": baseline_p99,
            "baseline_p999": baseline_p999,
            "degradation_start_s": None,
            "degradation_end_s": None,
            "degradation_duration_s": 0.0,
            "degradation_duration_ms": 0.0,
            "degradation_span_s": 0.0,
            "longest_degradation_window_s": 0.0,
            "min_ops_during": ts["count"].min(),
            "ops_drop_pct": 0.0,
            "peak_p99_during": ts["p99"].max(),
            "peak_p999_during": ts["p999"].max(),
            "recovery_detected": True,
            "pod_restart_detected": False,
            "degradation_windows": [],
        }

    windows = _degradation_windows(degraded)
    deg_start = int(windows[0]["start"])
    deg_end = int(windows[-1]["end"])
    duration_s = int(sum(window["duration_s"] for window in windows))
    span_s = deg_end - deg_start + 1
    longest_window_s = int(max(window["duration_s"] for window in windows))

    window = degraded
    min_ops = window["count"].min()
    ops_drop_pct = (1.0 - min_ops / baseline_ops) * 100.0
    peak_p99 = window["p99"].max()
    peak_p999 = window["p999"].max()

    zero_ops_seconds = (window["count"] == 0).sum()
    pod_restart = zero_ops_seconds >= 2

    recovery_threshold_ops = baseline_ops * RECOVERY_THRESHOLD
    post_stress = ts[ts["second"] > deg_end]
    if post_stress.empty:
        recovery = False
    else:
        last_10 = post_stress.tail(min(10, len(post_stress)))
        recovery = last_10["count"].mean() >= recovery_threshold_ops

    return {
        "degradation_detected": True,
        "baseline_ops": baseline_ops,
        "baseline_p99": baseline_p99,
        "baseline_p999": baseline_p999,
        "degradation_start_s": deg_start,
        "degradation_end_s": deg_end,
        "degradation_duration_s": float(duration_s),
        "degradation_duration_ms": float(duration_s * 1000),
        "degradation_span_s": float(span_s),
        "longest_degradation_window_s": float(longest_window_s),
        "min_ops_during": float(min_ops),
        "ops_drop_pct": ops_drop_pct,
        "peak_p99_during": peak_p99,
        "peak_p999_during": peak_p999,
        "recovery_detected": recovery,
        "pod_restart_detected": pod_restart,
        "degradation_windows": windows,
    }


def _degradation_windows(degraded: pd.DataFrame) -> List[Dict[str, int]]:
    seconds = [int(second) for second in degraded["second"].tolist()]
    if not seconds:
        return []

    windows = []
    start = previous = seconds[0]
    for second in seconds[1:]:
        if second == previous + 1:
            previous = second
            continue

        windows.append({
            "start": start,
            "end": previous,
            "duration_s": previous - start + 1,
        })
        start = previous = second

    windows.append({
        "start": start,
        "end": previous,
        "duration_s": previous - start + 1,
    })
    return windows


def _trim_terminal_partial_bucket(ts: pd.DataFrame, baseline_ops: float) -> pd.DataFrame:
    """Ignore the final short bucket emitted when memtier exits."""
    if len(ts) < 2:
        return ts

    last = ts.iloc[-1]
    prev = ts.iloc[-2]
    threshold = baseline_ops * DEGRADATION_THRESHOLD

    if last["count"] < threshold and prev["count"] >= threshold:
        return ts.iloc[:-1].copy()

    return ts


def _empty_result() -> Dict[str, Any]:
    return {
        "degradation_detected": False,
        "baseline_ops": None,
        "baseline_p99": None,
        "baseline_p999": None,
        "degradation_start_s": None,
        "degradation_end_s": None,
        "degradation_duration_s": None,
        "degradation_duration_ms": None,
        "degradation_span_s": None,
        "longest_degradation_window_s": None,
        "min_ops_during": None,
        "ops_drop_pct": None,
        "peak_p99_during": None,
        "peak_p999_during": None,
        "recovery_detected": None,
        "pod_restart_detected": None,
        "degradation_windows": [],
    }


def analyse_resilience_runs(
    json_dir: Path, scenario: str,
) -> Tuple[pd.DataFrame, List[pd.DataFrame]]:
    """Parse resilience_{scenario}_run_*.json files."""
    prefixes = {
        "cpu": "resilience_cpu",
        "memory": "resilience_mem",
        "memory-extreme": "resilience_mem_extreme",
        "maxmemory": "resilience_maxmemory",
    }
    prefix = prefixes[scenario]
    files = sorted(json_dir.glob(f"{prefix}_run_*.json"))
    if not files:
        raise FileNotFoundError(
            f"No {prefix}_run_*.json files in {json_dir}"
        )

    results = []
    all_ts = []

    for f in files:
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
        if not ts.empty:
            available_baseline = min(BASELINE_SECONDS, len(ts) // 2)
            if available_baseline >= 3:
                baseline_ops = ts[ts["second"] < available_baseline]["count"].mean()
                if baseline_ops > 0:
                    ts = _trim_terminal_partial_bucket(ts, baseline_ops)
        all_ts.append(ts)
        event_start_s, event_end_s = _event_window_for_file(json_dir, f, scenario)
        metrics = detect_degradation(ts)
        metrics["file"] = f.name
        metrics["scenario"] = scenario
        metrics["event_start_s"] = event_start_s
        metrics["event_end_s"] = event_end_s
        metrics["event_duration_s"] = (
            event_end_s - event_start_s
            if event_start_s is not None and event_end_s is not None
            else None
        )
        metrics["event_end_observed"] = (
            bool(not ts.empty and event_end_s <= ts["second"].max())
            if event_end_s is not None
            else None
        )
        results.append(metrics)

    return pd.DataFrame(results), all_ts


def _event_window_for_file(
    json_dir: Path,
    result_file: Path,
    scenario: str,
) -> Tuple[Optional[float], Optional[float]]:
    if scenario != "maxmemory":
        start = float(STRESS_START_SECOND)
        return start, start + float(STRESS_DURATIONS[scenario])

    run_idx = result_file.stem.rsplit("_", 1)[-1]
    timing_file = json_dir / f"maxmemory_resilience_timing_{run_idx}.json"
    if not timing_file.exists():
        start = float(STRESS_START_SECOND)
        return start, start + float(STRESS_DURATIONS[scenario])

    with timing_file.open() as fh:
        timing = json.load(fh)

    start = float(timing.get("steady_state_wait_s", STRESS_START_SECOND))
    duration = float(timing.get("pressure_duration_s", STRESS_DURATIONS[scenario]))
    return start, start + duration


def print_resilience_summary(df: pd.DataFrame, scenario: str) -> None:
    detected = df[df["degradation_detected"] == True]
    recovered = df[df["recovery_detected"] == True]
    restarts = df[df["pod_restart_detected"] == True]

    print(f"\nResilience [{scenario}] runs analysed: {len(df)}")
    print(f"Degradation detected: {len(detected)}/{len(df)} runs")
    print(f"Recovery after stress: {len(recovered)}/{len(df)} runs")
    print(f"Pod restart (OOM):     {len(restarts)}/{len(df)} runs")

    if detected.empty:
        print("No degradation events detected.")
        return

    metrics = [
        ("Total degraded time (ms)", "degradation_duration_ms"),
        ("Longest degraded window (s)", "longest_degradation_window_s"),
        ("Degradation span (s)", "degradation_span_s"),
        ("Min ops/sec during stress", "min_ops_during"),
        ("Ops drop (%)", "ops_drop_pct"),
        ("Baseline ops/sec", "baseline_ops"),
        ("Peak p99 during stress (ms)", "peak_p99_during"),
        ("Baseline p99 (ms)", "baseline_p99"),
        ("Peak p99.9 during stress (ms)", "peak_p999_during"),
        ("Baseline p99.9 (ms)", "baseline_p999"),
    ]

    print(f"\n{'Metric':<40} {'Mean':>12} {'Std':>12}")
    print("-" * 66)
    for label, col in metrics:
        mean = detected[col].mean()
        std = detected[col].std()
        print(f"{label:<40} {mean:>12.2f} {std:>12.2f}")


def save_resilience_csv(df: pd.DataFrame, out_dir: Path, scenario: str) -> None:
    csv_path = out_dir / f"resilience_{scenario}_summary.csv"
    df.to_csv(csv_path, index=False)
    print(f"\nResilience CSV saved to {csv_path}")


def plot_resilience_timeseries(
    all_ts: List[pd.DataFrame],
    results_df: pd.DataFrame,
    out_dir: Path,
    scenario: str,
) -> None:
    """Plot ops/sec and latency over time, highlighting degradation window."""
    labels = {
        "cpu": "CPU stress",
        "memory": "Memory stress",
        "memory-extreme": "Extreme memory stress",
        "maxmemory": "Maxmemory pressure",
    }
    label = labels[scenario]
    for i, (ts, (_, row)) in enumerate(zip(all_ts, results_df.iterrows())):
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
        stress_start, stress_end = _visible_event_window(row, ts, scenario)

        ax1.plot(ts["second"], ts["count"], linewidth=0.8, color="#4c72b0")
        ax1.set_ylabel("Ops/sec")
        ax1.set_title(f"Resilience [{label}] Run {i + 1}")

        if row.get("degradation_detected") and row["degradation_start_s"] is not None:
            ax1.axhline(
                row["baseline_ops"], linestyle="--", color="gray",
                linewidth=0.7, label="Baseline",
            )

        mark_test_window(ax1, stress_start, stress_end, with_labels=True)
        ax1.legend(fontsize=8)

        ax2.plot(ts["second"], ts["p99"], linewidth=0.8, color="#c44e52", label="p99")
        ax2.plot(ts["second"], ts["p50"], linewidth=0.8, color="#55a868", label="p50")
        ax2.set_ylabel("Latency (ms)")
        ax2.set_xlabel("Time (s)")
        ax2.legend(fontsize=8)

        mark_test_window(ax2, stress_start, stress_end)

        fig.tight_layout()
        plot_prefixes = {
            "cpu": "cpu",
            "memory": "mem",
            "memory-extreme": "mem_extreme",
            "maxmemory": "maxmemory",
        }
        prefix = plot_prefixes[scenario]
        fig.savefig(
            out_dir / f"resilience_{prefix}_run_{i + 1}.png",
            dpi=150, bbox_inches="tight",
        )
        plt.close(fig)


def plot_resilience_memory_timeseries(
    json_dir: Path,
    results_df: pd.DataFrame,
    out_dir: Path,
    scenario: str,
    prom: Optional[Any],
    query: str,
) -> None:
    """Plot per-second Valkey memory usage from Prometheus."""
    if prom is None:
        return

    prefixes = {
        "cpu": "cpu",
        "memory": "mem",
        "memory-extreme": "mem_extreme",
        "maxmemory": "maxmemory",
    }
    prefix = prefixes[scenario]

    for run_idx, (_, row) in enumerate(results_df.iterrows(), start=1):
        file_name = row.get("file")
        if not isinstance(file_name, str):
            continue

        run_path = json_dir / file_name
        if not run_path.exists():
            continue

        run_result = _load_run_result(run_path)
        runtime = run_result.get("Runtime", {})
        start_ms = runtime.get("Start time")
        finish_ms = runtime.get("Finish time")
        if start_ms is None or finish_ms is None:
            print(f"  [warn] No runtime timestamps in {file_name}; skipping memory plot")
            continue

        start_epoch = float(start_ms) / 1000.0
        start_dt = datetime.fromtimestamp(start_epoch, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(float(finish_ms) / 1000.0, tz=timezone.utc)

        try:
            prom_data = prom.custom_query_range(
                query=query,
                start_time=start_dt,
                end_time=end_dt,
                step="1s",
            )
        except Exception as e:
            print(f"  [warn] memory time-series query failed for {file_name}: {e}")
            continue

        memory_df = _prometheus_memory_frame(prom_data, start_epoch)
        if memory_df.empty:
            print(f"  [warn] Prometheus returned no memory samples for {file_name}")
            continue

        csv_path = out_dir / f"resilience_{prefix}_memory_run_{run_idx}.csv"
        memory_df.to_csv(csv_path, index=False)

        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(
            memory_df["second"],
            memory_df["total_memory_mb"],
            linewidth=1.0,
            color="#8172b2",
            label="Cluster memory",
        )
        ax.set_xlabel("Time (s)")
        ax.set_ylabel("Memory (MiB)")
        ax.set_title(f"Valkey memory usage | Run {run_idx}")

        stress_start, stress_end = _visible_event_window(row, memory_df, scenario)
        mark_test_window(ax, stress_start, stress_end, with_labels=True)
        ax.legend(fontsize=8)
        fig.tight_layout()
        fig.savefig(
            out_dir / f"resilience_{prefix}_memory_run_{run_idx}.png",
            dpi=150,
            bbox_inches="tight",
        )
        plt.close(fig)
        print(f"Memory time series saved to {csv_path}")


def _load_run_result(path: Path) -> Dict[str, Any]:
    with path.open() as fh:
        doc = json.load(fh)

    run_result = doc.get("RUN #1 RESULTS") or doc.get("ALL STATS")
    if run_result is not None:
        return run_result

    for key, value in doc.items():
        if key.startswith("RUN #") and key.endswith("RESULTS") and isinstance(value, dict):
            return value

    return {}


def _prometheus_memory_frame(prom_data: List[Dict[str, Any]], start_epoch: float) -> pd.DataFrame:
    records: Dict[int, Dict[str, float]] = {}
    for series in prom_data:
        metric = series.get("metric", {})
        label = (
            metric.get("pod")
            or metric.get("instance")
            or metric.get("job")
            or "total"
        )
        for ts, value in series.get("values", []):
            second = max(0, int(round(float(ts) - start_epoch)))
            records.setdefault(second, {"second": second})
            records[second][f"{label}_memory_mb"] = float(value) / (1024 * 1024)

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame([records[k] for k in sorted(records)])
    memory_cols = [c for c in df.columns if c.endswith("_memory_mb")]
    df[memory_cols] = df[memory_cols].fillna(0.0)
    df["total_memory_mb"] = df[memory_cols].sum(axis=1)
    return df


def _visible_event_window(
    row: pd.Series,
    ts: pd.DataFrame,
    scenario: str,
) -> Tuple[Optional[float], Optional[float]]:
    start = row.get("event_start_s")
    end = row.get("event_end_s")

    if pd.isna(start):
        start = float(STRESS_START_SECOND)
    if pd.isna(end):
        end = start + float(STRESS_DURATIONS[scenario])

    if not ts.empty and end > ts["second"].max():
        end = None

    return float(start), None if end is None else float(end)


def plot_resilience_comparison(
    results_df: pd.DataFrame, out_dir: Path, scenario: str,
) -> None:
    """Bar chart comparing degradation severity across runs."""
    detected = results_df[results_df["degradation_detected"] == True]
    if detected.empty:
        return

    labels = {
        "cpu": "CPU stress",
        "memory": "Memory stress",
        "memory-extreme": "Extreme memory stress",
        "maxmemory": "Maxmemory pressure",
    }
    prefixes = {
        "cpu": "cpu",
        "memory": "mem",
        "memory-extreme": "mem_extreme",
        "maxmemory": "maxmemory",
    }
    label = labels[scenario]
    prefix = prefixes[scenario]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))

    x = np.arange(len(detected))

    ax1.bar(x, detected["ops_drop_pct"], color="#dd8452", edgecolor="black", linewidth=0.5)
    ax1.set_xticks(x)
    ax1.set_xticklabels([f"Run {j+1}" for j in range(len(detected))])
    ax1.set_ylabel("Ops drop (%)")
    ax1.set_title(f"Ops/sec drop  |  {label}")
    mean_drop = detected["ops_drop_pct"].mean()
    ax1.axhline(mean_drop, linestyle="--", color="gray", linewidth=0.7,
                label=f"Mean: {mean_drop:.1f}%")
    ax1.legend(fontsize=8)

    ax2.bar(x, detected["peak_p99_during"], color="#c44e52", edgecolor="black", linewidth=0.5,
            label="Peak p99 (stress)")
    ax2.bar(x, detected["baseline_p99"], color="#55a868", edgecolor="black", linewidth=0.5,
            alpha=0.5, label="Baseline p99")
    ax2.set_xticks(x)
    ax2.set_xticklabels([f"Run {j+1}" for j in range(len(detected))])
    ax2.set_ylabel("Latency (ms)")
    ax2.set_title(f"p99 latency  |  {label}")
    ax2.legend(fontsize=8)

    fig.tight_layout()
    fig.savefig(
        out_dir / f"resilience_{prefix}_comparison.png",
        dpi=150, bbox_inches="tight",
    )
    plt.close(fig)

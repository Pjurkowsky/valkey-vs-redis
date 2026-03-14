from pathlib import Path
import argparse
import json
from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, Optional, Tuple

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import numpy as np

from datetime import datetime, timezone

matplotlib.use("Agg")

RATIO_LABELS = {
    "0-1": "read-only",
    "1-0": "write-only",
    "1-1": "mixed 50/50",
}

MEM_QUERY = """
sum by (pod)(
  container_memory_working_set_bytes{
    pod=~"valkey-[0-5]"
  }
)
"""

CPU_QUERY = """
sum by (pod)(
  rate(container_cpu_usage_seconds_total{
    pod=~"valkey-[0-5]"
  }[30s])
)
"""

METRIC_COLS = ["ops_sec", "p50", "p95", "p99", "p999", "cpu_util", "memory_usage"]


@dataclass
class Metric:
    ops_sec: float
    p50: float
    p95: float
    p99: float
    p999: float
    cpu_util: float
    memory_usage: float


@dataclass
class RunConfig:
    cpu: int
    payload: int
    ratio: str


def _build_prom(url: str):
    from prometheus_api_client import PrometheusConnect
    return PrometheusConnect(url=url, disable_ssl=True)


def mean_from_range(prom, query: str, start: datetime, end: datetime) -> Dict[str, float]:
    data = prom.custom_query_range(
        query=query,
        start_time=start,
        end_time=end,
        step="1s",
    )
    means: Dict[str, float] = {}
    for series in data:
        label = series["metric"].get("pod", "total")
        values = [float(v[1]) for v in series["values"]]
        means[label] = np.mean(values) if values else 0.0
    return means


def parse_filename(filename: Path) -> RunConfig:
    cpu, payload, ratio = filename.stem.split("_")
    return RunConfig(int(cpu), int(payload), ratio)


def extract_metric(
    run_result: Dict[str, Any],
    prom: Optional[Any] = None,
) -> Metric:
    runtime = run_result.get("Runtime", {})
    start_ms = runtime.get("Start time")
    finish_ms = runtime.get("Finish time")

    totals = run_result.get("Totals", {})
    ops_sec = totals.get("Ops/sec", 0.0)

    percentile_block = totals.get("Percentile Latencies", {})
    p50 = percentile_block.get("p50.00", float("nan"))
    p95 = percentile_block.get("p95.00", float("nan"))
    p99 = percentile_block.get("p99.00", float("nan"))
    p999 = percentile_block.get("p99.90", float("nan"))

    cpu_util = float("nan")
    memory_usage = float("nan")

    if prom is not None and start_ms and finish_ms:
        start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(finish_ms / 1000, tz=timezone.utc)
        try:
            mem_mean = mean_from_range(prom, MEM_QUERY, start_dt, end_dt)
            memory_usage = sum(v for v in mem_mean.values() if v is not None)
        except Exception as e:
            print(f"  [warn] memory query failed: {e}")

        try:
            cpu_mean = mean_from_range(prom, CPU_QUERY, start_dt, end_dt)
            cpu_util = sum(v for v in cpu_mean.values() if v is not None)
        except Exception as e:
            print(f"  [warn] cpu query failed: {e}")

    return Metric(ops_sec, p50, p95, p99, p999, cpu_util, memory_usage)


def iter_run_results(doc: Dict[str, Any]) -> Iterable[Tuple[int, Dict[str, Any]]]:
    for i in range(1, 6):
        key = f"RUN #{i} RESULTS"
        if key in doc and isinstance(doc[key], dict):
            yield i, doc[key]


def ratio_label(r: str) -> str:
    return RATIO_LABELS.get(r, r)


# ---------------------------------------------------------------------------
# Visualization helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Analyse memtier_benchmark JSON results")
    ap.add_argument("--input", required=True, help="Directory with benchmark JSON files")
    ap.add_argument("--output-dir", default="./plots", help="Directory to save plots and CSV")
    ap.add_argument("--prometheus-url", default="http://localhost:9090",
                    help="Prometheus base URL")
    ap.add_argument("--no-prometheus", action="store_true",
                    help="Skip Prometheus queries (CPU/memory will be NaN)")
    args = ap.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    prom = None
    if not args.no_prometheus:
        try:
            prom = _build_prom(args.prometheus_url)
            prom.check_prometheus_connection()
            print(f"Connected to Prometheus at {args.prometheus_url}")
        except Exception as e:
            print(f"[warn] Prometheus unavailable ({e}); CPU/memory metrics will be NaN")
            prom = None

    directory = Path(args.input)
    rows = []
    for file in sorted(directory.glob("*.json")):
        config = parse_filename(file)
        print(f"Processing {file.name}  (cpu={config.cpu}, payload={config.payload}KB, ratio={config.ratio})")

        with file.open("r") as f:
            doc = json.load(f)

        for run_id, run_result in iter_run_results(doc):
            metric = extract_metric(run_result, prom=prom)
            rows.append({
                "id": run_id,
                **asdict(config),
                **asdict(metric),
            })

    df = pd.DataFrame(rows)

    agg = (
        df.groupby(["cpu", "payload", "ratio"], as_index=False)
        .agg(
            n=("id", "count"),
            **{f"{m}_mean": (m, "mean") for m in METRIC_COLS},
            **{f"{m}_std": (m, "std") for m in METRIC_COLS},
        )
    )

    save_summary_csv(agg, out_dir)
    plot_ops_sec_bars(agg, out_dir)
    plot_latency_bars(agg, out_dir)
    plot_cpu_mem(agg, out_dir)
    plot_heatmap(agg, out_dir)

    print(f"\nAll plots saved to {out_dir}/")


if __name__ == "__main__":
    main()

import argparse
import json
from dataclasses import asdict
from pathlib import Path

import pandas as pd

from src.metrics import build_prom, extract_metric, iter_run_results, parse_filename
from src.models import METRIC_COLS
from src.plots import (
    plot_cpu_mem,
    plot_heatmap,
    plot_latency_bars,
    plot_ops_sec_bars,
    save_summary_csv,
)


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
            prom = build_prom(args.prometheus_url)
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

"""Unified CLI for Valkey vs Redis benchmark analysis."""

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

import pandas as pd


PLOTS_DIR = "./plots"


def cmd_benchmark(args: argparse.Namespace) -> None:
    from src.metrics import (
        build_prom,
        extract_cpu_util_by_pod,
        extract_metric,
        iter_run_results,
        parse_filename,
    )
    from src.models import METRIC_COLS
    from src.plots import (
        plot_cpu_mem,
        plot_cpu_util_per_pod,
        plot_heatmap,
        plot_latency_bars,
        plot_ops_sec_bars,
        save_summary_csv,
    )

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
    pod_cpu_rows = []
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
            for pod, cpu_util in extract_cpu_util_by_pod(run_result, prom=prom).items():
                pod_cpu_rows.append({
                    "id": run_id,
                    **asdict(config),
                    "pod": pod,
                    "cpu_util": cpu_util,
                })

    df = pd.DataFrame(rows)
    pod_cpu_df = pd.DataFrame(pod_cpu_rows)
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
    if not pod_cpu_df.empty:
        pod_cpu_df.to_csv(out_dir / "cpu_util_per_pod.csv", index=False)
        plot_cpu_util_per_pod(pod_cpu_df, out_dir)
    plot_heatmap(agg, out_dir)
    print(f"\nAll plots saved to {out_dir}/")


def cmd_failover(args: argparse.Namespace) -> None:
    from src.failover import (
        analyse_failover_runs,
        plot_failover_comparison,
        plot_failover_timeseries,
        print_failover_summary,
        save_failover_csv,
    )

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    results_df, all_ts = analyse_failover_runs(Path(args.input))
    print_failover_summary(results_df)
    save_failover_csv(results_df, out_dir)
    plot_failover_timeseries(all_ts, results_df, out_dir)
    plot_failover_comparison(results_df, out_dir)
    print(f"\nAll failover plots saved to {out_dir}/")


def cmd_resilience(args: argparse.Namespace) -> None:
    from src.resilience import (
        analyse_resilience_runs,
        plot_resilience_comparison,
        plot_resilience_timeseries,
        print_resilience_summary,
        save_resilience_csv,
    )

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    results_df, all_ts = analyse_resilience_runs(Path(args.input), args.scenario)
    print_resilience_summary(results_df, args.scenario)
    save_resilience_csv(results_df, out_dir, args.scenario)
    plot_resilience_timeseries(all_ts, results_df, out_dir, args.scenario)
    plot_resilience_comparison(results_df, out_dir, args.scenario)
    print(f"\nAll resilience [{args.scenario}] plots saved to {out_dir}/")


def cmd_upgrade(args: argparse.Namespace) -> None:
    from src.upgrade import (
        analyse_upgrade_runs,
        plot_upgrade_comparison,
        plot_upgrade_timeseries,
        print_upgrade_summary,
        save_upgrade_csv,
    )

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading upgrade results from {args.input} ...")
    results_df, all_ts, all_windows = analyse_upgrade_runs(Path(args.input))

    print_upgrade_summary(results_df)
    save_upgrade_csv(results_df, out_dir)
    plot_upgrade_timeseries(all_ts, all_windows, results_df, out_dir)
    plot_upgrade_comparison(results_df, out_dir)
    print(f"\nPlots saved to {out_dir}/")


def cmd_consistency(args: argparse.Namespace) -> None:
    from src.consistency import (
        analyse_consistency_runs,
        plot_consistency_comparison,
        plot_consistency_timeseries,
        print_consistency_summary,
        save_consistency_csv,
    )

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading consistency results from {args.input} ...")
    results_df, all_ts = analyse_consistency_runs(Path(args.input))

    print_consistency_summary(results_df)
    save_consistency_csv(results_df, out_dir)
    plot_consistency_timeseries(all_ts, results_df, out_dir)
    plot_consistency_comparison(results_df, out_dir)
    print(f"\nPlots saved to {out_dir}/")


def cmd_reshard(args: argparse.Namespace) -> None:
    from src.reshard import (
        analyse_reshard_runs,
        plot_reshard_comparison,
        plot_reshard_timeseries,
        print_reshard_summary,
        save_reshard_csv,
    )

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading reshard results from {args.input} ...")
    results_df, all_ts, all_windows, all_timings = analyse_reshard_runs(Path(args.input))

    print_reshard_summary(results_df)
    save_reshard_csv(results_df, out_dir)
    plot_reshard_timeseries(all_ts, all_windows, all_timings, results_df, out_dir)
    plot_reshard_comparison(results_df, out_dir)
    print(f"\nPlots saved to {out_dir}/")


def cmd_split_brain(args: argparse.Namespace) -> None:
    from src.split_brain import (
        analyse_split_brain_runs,
        plot_split_brain_comparison,
        plot_split_brain_timeseries,
        print_split_brain_summary,
        save_split_brain_csv,
    )

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading split-brain results from {args.input} ...")
    results_df, all_ts = analyse_split_brain_runs(Path(args.input))

    print_split_brain_summary(results_df)
    save_split_brain_csv(results_df, out_dir)
    plot_split_brain_timeseries(all_ts, results_df, out_dir)
    plot_split_brain_comparison(results_df, out_dir)
    print(f"\nPlots saved to {out_dir}/")


def cmd_backup(args: argparse.Namespace) -> None:
    from src.backup_restore import (
        analyse_backup_runs,
        plot_backup_restore_bars,
        plot_restore_by_size,
        print_backup_summary,
        save_backup_csv,
    )

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading backup/restore results from {args.input} ...")
    results_df = analyse_backup_runs(Path(args.input))

    print_backup_summary(results_df)
    save_backup_csv(results_df, out_dir)
    plot_backup_restore_bars(results_df, out_dir)
    plot_restore_by_size(results_df, out_dir)
    print(f"\nPlots saved to {out_dir}/")


def cmd_backup_snapshot(args: argparse.Namespace) -> None:
    from src.backup_pvc_snapshot import (
        analyse_backup_snapshot_runs,
        plot_backup_comparison,
        plot_valkey_phase_breakdown,
        print_backup_snapshot_summary,
        save_backup_snapshot_csv,
    )

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading PVC snapshot backup results from {args.input} ...")
    vk_df, ms_df = analyse_backup_snapshot_runs(Path(args.input))

    print_backup_snapshot_summary(vk_df, ms_df)
    save_backup_snapshot_csv(vk_df, ms_df, out_dir)
    plot_backup_comparison(vk_df, ms_df, out_dir)
    plot_valkey_phase_breakdown(vk_df, out_dir)
    print(f"\nPlots saved to {out_dir}/")


def cmd_maxmemory(args: argparse.Namespace) -> None:
    from src.maxmemory import (
        analyse_maxmemory_runs,
        plot_evictions_and_missing,
        plot_memory_before_after,
        print_maxmemory_summary,
        save_maxmemory_csv,
    )

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading maxmemory results from {args.input} ...")
    results_df = analyse_maxmemory_runs(Path(args.input))

    print_maxmemory_summary(results_df)
    save_maxmemory_csv(results_df, out_dir)
    plot_memory_before_after(results_df, out_dir)
    plot_evictions_and_missing(results_df, out_dir)
    print(f"\nPlots saved to {out_dir}/")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cli.py",
        description="Valkey vs Redis — unified benchmark analysis CLI",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # benchmark
    p = sub.add_parser("benchmark", help="Performance benchmark analysis")
    p.add_argument("--input", required=True, help="Directory with benchmark JSON files")
    p.add_argument("--output-dir", default=f"{PLOTS_DIR}/benchmark", help="Output directory")
    p.add_argument("--prometheus-url", default="http://localhost:9090",
                   help="Prometheus base URL")
    p.add_argument("--no-prometheus", action="store_true",
                   help="Skip Prometheus queries")
    p.set_defaults(func=cmd_benchmark)

    # failover
    p = sub.add_parser("failover", help="Failover analysis")
    p.add_argument("--input", required=True, help="Directory with failover_run_*.json files")
    p.add_argument("--output-dir", default=f"{PLOTS_DIR}/failover", help="Output directory")
    p.set_defaults(func=cmd_failover)

    # resilience
    p = sub.add_parser("resilience", help="Resilience analysis")
    p.add_argument("--input", required=True, help="Directory with resilience_*_run_*.json files")
    p.add_argument("--scenario", required=True, choices=["cpu", "memory", "memory-extreme", "maxmemory"],
                   help="Stress scenario to analyse")
    p.add_argument("--output-dir", default=f"{PLOTS_DIR}/resilience", help="Output directory")
    p.set_defaults(func=cmd_resilience)

    # upgrade
    p = sub.add_parser("upgrade", help="Zero-downtime upgrade analysis")
    p.add_argument("--input", required=True, help="Directory with upgrade_run_*.json files")
    p.add_argument("--output-dir", default=f"{PLOTS_DIR}/upgrade", help="Output directory")
    p.set_defaults(func=cmd_upgrade)

    # consistency
    p = sub.add_parser("consistency", help="Data consistency analysis")
    p.add_argument("--input", required=True, help="Directory with consistency_run_*.json files")
    p.add_argument("--output-dir", default=f"{PLOTS_DIR}/consistency", help="Output directory")
    p.set_defaults(func=cmd_consistency)

    # split-brain
    p = sub.add_parser("split-brain", help="Split-brain consistency analysis")
    p.add_argument("--input", required=True, help="Directory with split_brain_run_*.json files")
    p.add_argument("--output-dir", default=f"{PLOTS_DIR}/split_brain", help="Output directory")
    p.set_defaults(func=cmd_split_brain)

    # reshard
    p = sub.add_parser("reshard", help="Horizontal scaling / resharding analysis")
    p.add_argument("--input", required=True, help="Directory with reshard_run/timing_*.json files")
    p.add_argument("--output-dir", default=f"{PLOTS_DIR}/reshard", help="Output directory")
    p.set_defaults(func=cmd_reshard)

    # backup
    p = sub.add_parser("backup", help="Backup & restore analysis")
    p.add_argument("--input", required=True, help="Directory with backup_timing_*.json files")
    p.add_argument("--output-dir", default=f"{PLOTS_DIR}/backup", help="Output directory")
    p.set_defaults(func=cmd_backup)

    # backup-snapshot
    p = sub.add_parser("backup-snapshot", help="PVC snapshot backup & Memorystore backup analysis")
    p.add_argument("--input", required=True,
                   help="Directory with vk/ and ms/ subdirectories containing benchmark results")
    p.add_argument("--output-dir", default=f"{PLOTS_DIR}/backup_snapshot", help="Output directory")
    p.set_defaults(func=cmd_backup_snapshot)

    # maxmemory
    p = sub.add_parser("maxmemory", help="Maxmemory eviction analysis")
    p.add_argument("--input", required=True, help="Directory with maxmemory_summary_*.json files")
    p.add_argument("--output-dir", default=f"{PLOTS_DIR}/maxmemory", help="Output directory")
    p.set_defaults(func=cmd_maxmemory)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

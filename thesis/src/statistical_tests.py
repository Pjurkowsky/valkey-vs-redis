#!/usr/bin/env python3
"""
Statistical tests for Valkey vs Redis 7.2 (vs Memorystore) benchmark data.

For each scenario, performs:
- Descriptive statistics (median, mean, std, IQR)
- Shapiro-Wilk normality test
- Mann-Whitney U test (primary, non-parametric)
- Welch t-test (supplementary, parametric)
- Effect size: rank-biserial correlation r (for Mann-Whitney)
                and Cohen's d (for t-test)
- Benjamini-Hochberg FDR correction for multiple comparisons

Usage:
    python statistical_tests.py [--results-dir PATH] [--output-dir PATH]
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ALPHA = 0.05


def rank_biserial_r(u_stat: float, n1: int, n2: int) -> float:
    """Rank-biserial correlation as effect size for Mann-Whitney U."""
    return 1 - (2 * u_stat) / (n1 * n2)


def cohens_d(group1: np.ndarray, group2: np.ndarray) -> float:
    """Cohen's d for two independent samples (pooled std)."""
    n1, n2 = len(group1), len(group2)
    var1, var2 = group1.var(ddof=1), group2.var(ddof=1)
    pooled_std = np.sqrt(((n1 - 1) * var1 + (n2 - 1) * var2) / (n1 + n2 - 2))
    if pooled_std == 0:
        return 0.0
    return (group1.mean() - group2.mean()) / pooled_std


def benjamini_hochberg(p_values: np.ndarray) -> np.ndarray:
    """Benjamini-Hochberg FDR correction."""
    n = len(p_values)
    sorted_idx = np.argsort(p_values)
    sorted_p = p_values[sorted_idx]
    adjusted = np.empty(n)
    cummin = 1.0
    for i in range(n - 1, -1, -1):
        val = sorted_p[i] * n / (i + 1)
        cummin = min(cummin, val)
        adjusted[sorted_idx[i]] = min(cummin, 1.0)
    return adjusted


def run_two_sample_test(
    group1: np.ndarray,
    group2: np.ndarray,
    label1: str = "Valkey",
    label2: str = "Redis 7.2",
) -> dict:
    """Run Mann-Whitney U + Welch t-test on two groups. Returns result dict."""
    n1, n2 = len(group1), len(group2)

    result = {
        "n1": n1,
        "n2": n2,
        f"{label1}_median": float(np.median(group1)),
        f"{label1}_mean": float(np.mean(group1)),
        f"{label1}_std": float(np.std(group1, ddof=1)),
        f"{label2}_median": float(np.median(group2)),
        f"{label2}_mean": float(np.mean(group2)),
        f"{label2}_std": float(np.std(group2, ddof=1)),
    }

    # Shapiro-Wilk (informational only)
    if n1 >= 3:
        _, p_shapiro1 = stats.shapiro(group1)
        result[f"shapiro_p_{label1}"] = float(p_shapiro1)
    if n2 >= 3:
        _, p_shapiro2 = stats.shapiro(group2)
        result[f"shapiro_p_{label2}"] = float(p_shapiro2)

    # Mann-Whitney U (primary test)
    u_stat, p_mann = stats.mannwhitneyu(group1, group2, alternative="two-sided")
    result["mann_whitney_U"] = float(u_stat)
    result["mann_whitney_p"] = float(p_mann)
    result["rank_biserial_r"] = float(rank_biserial_r(u_stat, n1, n2))

    # Welch t-test (supplementary)
    t_stat, p_ttest = stats.ttest_ind(group1, group2, equal_var=False)
    result["welch_t"] = float(t_stat)
    result["welch_p"] = float(p_ttest)
    result["cohens_d"] = float(cohens_d(group1, group2))

    # Significance flags
    result["significant_mann_whitney"] = p_mann < ALPHA
    result["significant_welch"] = p_ttest < ALPHA

    return result


def test_performance(results_dir: Path) -> pd.DataFrame:
    """Performance benchmark: Valkey vs Redis 7.2, 18 config cells."""
    print("\n" + "=" * 70)
    print("PERFORMANCE (memtier throughput/latency)")
    print("=" * 70)

    vk_csv = results_dir / "perf" / "valkey" / "runs.csv"
    rd_csv = results_dir / "perf" / "redis72" / "runs.csv"

    if not vk_csv.exists() or not rd_csv.exists():
        print("  [SKIP] runs.csv not found for both systems")
        return pd.DataFrame()

    vk = pd.read_csv(vk_csv)
    rd = pd.read_csv(rd_csv)

    rows = []
    configs = vk.groupby(["cpu", "payload", "ratio"]).groups.keys()

    for cpu, payload, ratio in sorted(configs):
        vk_ops = vk[(vk["cpu"] == cpu) & (vk["payload"] == payload) & (vk["ratio"] == ratio)]["ops_sec"].values
        rd_ops = rd[(rd["cpu"] == cpu) & (rd["payload"] == payload) & (rd["ratio"] == ratio)]["ops_sec"].values

        if len(vk_ops) < 3 or len(rd_ops) < 3:
            continue

        res = run_two_sample_test(vk_ops, rd_ops)
        res["metric"] = "ops_sec"
        res["cpu"] = cpu
        res["payload_kb"] = payload
        res["ratio"] = ratio
        rows.append(res)

        # Also test p99 latency
        vk_p99 = vk[(vk["cpu"] == cpu) & (vk["payload"] == payload) & (vk["ratio"] == ratio)]["p99"].values
        rd_p99 = rd[(rd["cpu"] == cpu) & (rd["payload"] == payload) & (rd["ratio"] == ratio)]["p99"].values

        if len(vk_p99) >= 3 and len(rd_p99) >= 3:
            res_lat = run_two_sample_test(vk_p99, rd_p99)
            res_lat["metric"] = "p99_latency_ms"
            res_lat["cpu"] = cpu
            res_lat["payload_kb"] = payload
            res_lat["ratio"] = ratio
            rows.append(res_lat)

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Apply BH FDR correction separately for ops_sec and p99
    for metric in ["ops_sec", "p99_latency_ms"]:
        mask = df["metric"] == metric
        if mask.sum() > 1:
            p_vals = df.loc[mask, "mann_whitney_p"].values
            df.loc[mask, "mann_whitney_p_adjusted"] = benjamini_hochberg(p_vals)
            df.loc[mask, "significant_after_fdr"] = df.loc[mask, "mann_whitney_p_adjusted"] < ALPHA
        else:
            df.loc[mask, "mann_whitney_p_adjusted"] = df.loc[mask, "mann_whitney_p"]
            df.loc[mask, "significant_after_fdr"] = df.loc[mask, "mann_whitney_p"] < ALPHA

    print_performance_summary(df)
    return df


def print_performance_summary(df: pd.DataFrame):
    """Print readable performance test results."""
    for metric in ["ops_sec", "p99_latency_ms"]:
        subset = df[df["metric"] == metric]
        if subset.empty:
            continue
        print(f"\n  Metric: {metric}")
        print(f"  {'Config':<20} {'U':>8} {'p':>10} {'p_adj':>10} {'r':>8} {'Sig?':>6}")
        print(f"  {'-'*20} {'-'*8} {'-'*10} {'-'*10} {'-'*8} {'-'*6}")
        for _, row in subset.iterrows():
            config = f"{int(row['cpu'])}vCPU/{int(row['payload_kb'])}KB/{row['ratio']}"
            sig = "YES" if row["significant_after_fdr"] else "no"
            print(
                f"  {config:<20} {row['mann_whitney_U']:>8.1f} "
                f"{row['mann_whitney_p']:>10.4f} {row['mann_whitney_p_adjusted']:>10.4f} "
                f"{row['rank_biserial_r']:>8.3f} {sig:>6}"
            )
    n_sig = df[df.get("significant_after_fdr", False) == True].shape[0]
    print(f"\n  Total comparisons: {len(df)}, Significant after FDR: {n_sig}")


def test_failover(results_dir: Path) -> pd.DataFrame:
    """Failover: Valkey vs Redis 7.2."""
    print("\n" + "=" * 70)
    print("FAILOVER (pod-failure chaos)")
    print("=" * 70)

    vk_csv = results_dir / "failover" / "vk" / "failover_summary.csv"
    rd_csv = results_dir / "failover" / "redis" / "failover_summary.csv"

    if not vk_csv.exists() or not rd_csv.exists():
        print("  [SKIP] failover_summary.csv not found")
        return pd.DataFrame()

    vk = pd.read_csv(vk_csv)
    rd = pd.read_csv(rd_csv)

    metrics = [
        "failover_duration_s",
        "chaos_window_drop_pct_observed",
        "error_window_duration_s",
    ]

    rows = []
    for metric in metrics:
        if metric not in vk.columns or metric not in rd.columns:
            continue
        vk_vals = vk[metric].dropna().values
        rd_vals = rd[metric].dropna().values
        if len(vk_vals) < 3 or len(rd_vals) < 3:
            continue
        res = run_two_sample_test(vk_vals, rd_vals)
        res["metric"] = metric
        rows.append(res)

    df = pd.DataFrame(rows)
    if not df.empty:
        print(f"\n  {'Metric':<35} {'U':>6} {'p':>10} {'r':>8} {'Sig?':>5}")
        print(f"  {'-'*35} {'-'*6} {'-'*10} {'-'*8} {'-'*5}")
        for _, row in df.iterrows():
            sig = "YES" if row["significant_mann_whitney"] else "no"
            print(
                f"  {row['metric']:<35} {row['mann_whitney_U']:>6.1f} "
                f"{row['mann_whitney_p']:>10.4f} {row['rank_biserial_r']:>8.3f} {sig:>5}"
            )
    return df


def test_backup_restore(results_dir: Path) -> pd.DataFrame:
    """Backup/restore: Valkey vs Redis vs Memorystore."""
    print("\n" + "=" * 70)
    print("BACKUP / RESTORE")
    print("=" * 70)

    vk_csv = results_dir / "backup" / "vk" / "online_replica_snapshot_summary.csv"
    rd_csv = results_dir / "backup" / "redis" / "online_replica_snapshot_summary.csv"
    ms_dir = results_dir / "backup" / "ms"

    rows = []

    # Load self-hosted data
    vk_data, rd_data, ms_data = None, None, None
    if vk_csv.exists():
        vk_data = pd.read_csv(vk_csv)
    if rd_csv.exists():
        rd_data = pd.read_csv(rd_csv)

    # Load Memorystore timing
    ms_backup_durations = []
    ms_restore_durations = []
    if ms_dir.exists():
        for f in sorted(ms_dir.glob("memorystore_backup_timing_*.json")):
            with open(f) as fh:
                d = json.load(fh)
                ms_backup_durations.append(d.get("backup_duration_s"))
                ms_restore_durations.append(d.get("restore_duration_s"))

    metrics_selfhosted = ["backup_duration_s", "restore_duration_s"]

    # Valkey vs Redis pairwise
    if vk_data is not None and rd_data is not None:
        for metric in metrics_selfhosted:
            if metric in vk_data.columns and metric in rd_data.columns:
                vk_vals = vk_data[metric].dropna().values
                rd_vals = rd_data[metric].dropna().values
                if len(vk_vals) >= 3 and len(rd_vals) >= 3:
                    res = run_two_sample_test(vk_vals, rd_vals)
                    res["metric"] = metric
                    res["comparison"] = "Valkey vs Redis 7.2"
                    rows.append(res)

    # Valkey vs Memorystore
    if vk_data is not None and ms_backup_durations:
        vk_backup = vk_data["backup_duration_s"].dropna().values
        ms_backup = np.array([x for x in ms_backup_durations if x is not None])
        if len(vk_backup) >= 3 and len(ms_backup) >= 3:
            res = run_two_sample_test(vk_backup, ms_backup, "Valkey", "Memorystore")
            res["metric"] = "backup_duration_s"
            res["comparison"] = "Valkey vs Memorystore"
            rows.append(res)

    if vk_data is not None and ms_restore_durations:
        vk_restore = vk_data["restore_duration_s"].dropna().values
        ms_restore = np.array([x for x in ms_restore_durations if x is not None])
        if len(vk_restore) >= 3 and len(ms_restore) >= 3:
            res = run_two_sample_test(vk_restore, ms_restore, "Valkey", "Memorystore")
            res["metric"] = "restore_duration_s"
            res["comparison"] = "Valkey vs Memorystore"
            rows.append(res)

    # Redis vs Memorystore
    if rd_data is not None and ms_restore_durations:
        rd_restore = rd_data["restore_duration_s"].dropna().values
        ms_restore = np.array([x for x in ms_restore_durations if x is not None])
        if len(rd_restore) >= 3 and len(ms_restore) >= 3:
            res = run_two_sample_test(rd_restore, ms_restore, "Redis 7.2", "Memorystore")
            res["metric"] = "restore_duration_s"
            res["comparison"] = "Redis 7.2 vs Memorystore"
            rows.append(res)

    df = pd.DataFrame(rows)
    if not df.empty:
        # Kruskal-Wallis for 3-group comparison on restore_duration_s
        if vk_data is not None and rd_data is not None and ms_restore_durations:
            vk_r = vk_data["restore_duration_s"].dropna().values
            rd_r = rd_data["restore_duration_s"].dropna().values
            ms_r = np.array([x for x in ms_restore_durations if x is not None])
            if len(vk_r) >= 3 and len(rd_r) >= 3 and len(ms_r) >= 3:
                h_stat, p_kruskal = stats.kruskal(vk_r, rd_r, ms_r)
                print(f"\n  Kruskal-Wallis (restore_duration_s, 3 groups): H={h_stat:.3f}, p={p_kruskal:.4f}")

        print(f"\n  {'Comparison':<30} {'Metric':<22} {'U':>6} {'p':>10} {'r':>8} {'Sig?':>5}")
        print(f"  {'-'*30} {'-'*22} {'-'*6} {'-'*10} {'-'*8} {'-'*5}")
        for _, row in df.iterrows():
            sig = "YES" if row["significant_mann_whitney"] else "no"
            print(
                f"  {row['comparison']:<30} {row['metric']:<22} "
                f"{row['mann_whitney_U']:>6.1f} {row['mann_whitney_p']:>10.4f} "
                f"{row['rank_biserial_r']:>8.3f} {sig:>5}"
            )
    return df


def load_reshard_timings(base_dir: Path, prefix: str, phase: str) -> list[float]:
    """Load operation_duration_s from reshard timing files."""
    durations = []
    for i in range(1, 20):
        if phase == "up":
            fname = f"{prefix}_timing_{i}.json"
        else:
            fname = f"{prefix}_down_timing_{i}.json"
        fpath = base_dir / fname
        if fpath.exists():
            with open(fpath) as f:
                d = json.load(f)
                dur = d.get("operation_duration_s")
                if dur is not None:
                    durations.append(dur)
    return durations


def test_resharding(results_dir: Path) -> pd.DataFrame:
    """Resharding: Valkey (atomic/legacy) vs Redis vs Memorystore."""
    print("\n" + "=" * 70)
    print("RESHARDING (scale up/down)")
    print("=" * 70)

    atomic_dir = results_dir / "reshard" / "atomic"
    legacy_dir = results_dir / "reshard" / "legacy"
    redis_dir = results_dir / "reshard" / "redis"
    ms_dir = results_dir / "reshard" / "ms"

    rows = []

    for phase in ["up", "down"]:
        groups = {}

        atomic_vals = load_reshard_timings(atomic_dir, "reshard_atomic", phase)
        if atomic_vals:
            groups["Valkey (atomic)"] = np.array(atomic_vals)

        legacy_vals = load_reshard_timings(legacy_dir, "reshard_legacy", phase)
        if legacy_vals:
            groups["Valkey (legacy)"] = np.array(legacy_vals)

        redis_vals = load_reshard_timings(redis_dir, "reshard_redis72", phase)
        if redis_vals:
            groups["Redis 7.2"] = np.array(redis_vals)

        ms_vals = load_reshard_timings(ms_dir, "reshard", phase)
        if ms_vals:
            groups["Memorystore"] = np.array(ms_vals)

        # Kruskal-Wallis if 3+ groups
        group_arrays = [v for v in groups.values() if len(v) >= 3]
        if len(group_arrays) >= 3:
            h_stat, p_kruskal = stats.kruskal(*group_arrays)
            print(f"\n  Kruskal-Wallis (phase={phase}, {len(group_arrays)} groups): H={h_stat:.3f}, p={p_kruskal:.4f}")

        # Pairwise comparisons
        names = list(groups.keys())
        for i in range(len(names)):
            for j in range(i + 1, len(names)):
                g1, g2 = groups[names[i]], groups[names[j]]
                if len(g1) >= 3 and len(g2) >= 3:
                    res = run_two_sample_test(g1, g2, names[i], names[j])
                    res["metric"] = "operation_duration_s"
                    res["phase"] = phase
                    res["comparison"] = f"{names[i]} vs {names[j]}"
                    rows.append(res)

    df = pd.DataFrame(rows)
    if not df.empty:
        # BH correction per phase
        for phase in ["up", "down"]:
            mask = df["phase"] == phase
            if mask.sum() > 1:
                p_vals = df.loc[mask, "mann_whitney_p"].values
                df.loc[mask, "mann_whitney_p_adjusted"] = benjamini_hochberg(p_vals)
                df.loc[mask, "significant_after_fdr"] = df.loc[mask, "mann_whitney_p_adjusted"] < ALPHA
            else:
                df.loc[mask, "mann_whitney_p_adjusted"] = df.loc[mask, "mann_whitney_p"]
                df.loc[mask, "significant_after_fdr"] = df.loc[mask, "mann_whitney_p"] < ALPHA

        for phase in ["up", "down"]:
            subset = df[df["phase"] == phase]
            if subset.empty:
                continue
            print(f"\n  Phase: {phase}")
            print(f"  {'Comparison':<35} {'U':>6} {'p':>10} {'p_adj':>10} {'r':>8} {'Sig?':>5}")
            print(f"  {'-'*35} {'-'*6} {'-'*10} {'-'*10} {'-'*8} {'-'*5}")
            for _, row in subset.iterrows():
                sig = "YES" if row.get("significant_after_fdr", False) else "no"
                print(
                    f"  {row['comparison']:<35} {row['mann_whitney_U']:>6.1f} "
                    f"{row['mann_whitney_p']:>10.4f} {row['mann_whitney_p_adjusted']:>10.4f} "
                    f"{row['rank_biserial_r']:>8.3f} {sig:>5}"
                )
    return df


def extract_memtier_totals_ops(json_path: Path) -> float | None:
    """Extract Totals Ops/sec from a memtier JSON file."""
    with open(json_path) as f:
        data = json.load(f)
    # memtier format: "ALL STATS" > "Totals" > "Ops/sec"
    # or: top-level keys with "RUN #1" etc
    if "ALL STATS" in data:
        totals = data["ALL STATS"].get("Totals", {})
        return totals.get("Ops/sec")
    # Alternative: check for nested structure
    for key in data:
        if "Totals" in str(data.get(key, "")):
            if isinstance(data[key], dict) and "Totals" in data[key]:
                return data[key]["Totals"].get("Ops/sec")
    return None


def test_cpu_resilience(results_dir: Path) -> pd.DataFrame:
    """CPU resilience: Valkey vs Redis 7.2, stress on 1/2/3 masters."""
    print("\n" + "=" * 70)
    print("CPU RESILIENCE (stress on 1/2/3 masters)")
    print("=" * 70)

    base = results_dir / "resilliance" / "cpu"
    rows = []

    for m_count in [1, 2, 3]:
        vk_dir = base / "valkey" / f"m{m_count}"
        rd_dir = base / "redis" / f"m{m_count}"

        if not vk_dir.exists() or not rd_dir.exists():
            continue

        # Extract total ops/sec from memtier run files
        vk_ops, rd_ops = [], []
        for i in range(1, 10):
            vk_f = vk_dir / f"resilience_cpu_run_{i}.json"
            rd_f = rd_dir / f"resilience_cpu_run_{i}.json"
            if vk_f.exists():
                ops = extract_memtier_totals_ops(vk_f)
                if ops is not None:
                    vk_ops.append(ops)
            if rd_f.exists():
                ops = extract_memtier_totals_ops(rd_f)
                if ops is not None:
                    rd_ops.append(ops)

        if len(vk_ops) >= 3 and len(rd_ops) >= 3:
            res = run_two_sample_test(np.array(vk_ops), np.array(rd_ops))
            res["metric"] = "total_ops_sec_under_stress"
            res["masters_stressed"] = m_count
            rows.append(res)

    df = pd.DataFrame(rows)
    if not df.empty:
        print(f"\n  {'Masters stressed':<20} {'U':>6} {'p':>10} {'r':>8} {'d':>8} {'Sig?':>5}")
        print(f"  {'-'*20} {'-'*6} {'-'*10} {'-'*8} {'-'*8} {'-'*5}")
        for _, row in df.iterrows():
            sig = "YES" if row["significant_mann_whitney"] else "no"
            print(
                f"  {int(row['masters_stressed']):<20} {row['mann_whitney_U']:>6.1f} "
                f"{row['mann_whitney_p']:>10.4f} {row['rank_biserial_r']:>8.3f} "
                f"{row['cohens_d']:>8.3f} {sig:>5}"
            )
    return df


def test_memory_resilience(results_dir: Path) -> pd.DataFrame:
    """Memory (maxmemory) resilience: Valkey vs Redis 7.2."""
    print("\n" + "=" * 70)
    print("MEMORY RESILIENCE (maxmemory allkeys-lru)")
    print("=" * 70)

    base = results_dir / "resilliance" / "memory"
    vk_dir = base / "valkey" / "baseline"
    rd_dir = base / "redis" / "baseline"

    if not vk_dir.exists() or not rd_dir.exists():
        print("  [SKIP] directory not found")
        return pd.DataFrame()

    vk_data, rd_data = [], []
    for i in range(1, 10):
        vk_f = vk_dir / f"maxmemory_summary_valkey_allkeys-lru_{i}.json"
        rd_f = rd_dir / f"maxmemory_summary_redis72_allkeys-lru_{i}.json"
        if vk_f.exists():
            with open(vk_f) as f:
                vk_data.append(json.load(f))
        if rd_f.exists():
            with open(rd_f) as f:
                rd_data.append(json.load(f))

    if len(vk_data) < 3 or len(rd_data) < 3:
        print("  [SKIP] not enough runs")
        return pd.DataFrame()

    metrics = ["memtier_ops_sec", "memtier_p99_ms", "evicted_keys_delta"]
    rows = []
    for metric in metrics:
        vk_vals = np.array([d[metric] for d in vk_data if d.get(metric) is not None])
        rd_vals = np.array([d[metric] for d in rd_data if d.get(metric) is not None])
        if len(vk_vals) >= 3 and len(rd_vals) >= 3:
            res = run_two_sample_test(vk_vals, rd_vals)
            res["metric"] = metric
            rows.append(res)

    df = pd.DataFrame(rows)
    if not df.empty:
        print(f"\n  {'Metric':<25} {'U':>6} {'p':>10} {'r':>8} {'d':>8} {'Sig?':>5}")
        print(f"  {'-'*25} {'-'*6} {'-'*10} {'-'*8} {'-'*8} {'-'*5}")
        for _, row in df.iterrows():
            sig = "YES" if row["significant_mann_whitney"] else "no"
            print(
                f"  {row['metric']:<25} {row['mann_whitney_U']:>6.1f} "
                f"{row['mann_whitney_p']:>10.4f} {row['rank_biserial_r']:>8.3f} "
                f"{row['cohens_d']:>8.3f} {sig:>5}"
            )
    return df


def test_split_brain(results_dir: Path) -> pd.DataFrame:
    """Split-brain consistency: Valkey vs Redis 7.2."""
    print("\n" + "=" * 70)
    print("SPLIT-BRAIN (data consistency)")
    print("=" * 70)

    vk_dir = results_dir / "split_brain" / "vk"
    rd_dir = results_dir / "split_brain" / "redis"

    if not vk_dir.exists() or not rd_dir.exists():
        print("  [SKIP] directory not found")
        return pd.DataFrame()

    def load_split_brain(directory: Path) -> list[dict]:
        data = []
        for f in sorted(directory.glob("split_brain_run_*.json")):
            with open(f) as fh:
                data.append(json.load(fh))
        return data

    vk_data = load_split_brain(vk_dir)
    rd_data = load_split_brain(rd_dir)

    if len(vk_data) < 3 or len(rd_data) < 3:
        print("  [SKIP] not enough runs")
        return pd.DataFrame()

    metrics = ["keys_missing", "minority_loss_rate", "loss_rate", "total_failed"]
    rows = []
    for metric in metrics:
        vk_vals = np.array([d[metric] for d in vk_data if metric in d])
        rd_vals = np.array([d[metric] for d in rd_data if metric in d])
        if len(vk_vals) >= 3 and len(rd_vals) >= 3:
            res = run_two_sample_test(vk_vals, rd_vals)
            res["metric"] = metric
            rows.append(res)

    df = pd.DataFrame(rows)
    if not df.empty:
        print(f"\n  {'Metric':<25} {'VK median':>12} {'RD median':>12} {'U':>6} {'p':>10} {'r':>8} {'Sig?':>5}")
        print(f"  {'-'*25} {'-'*12} {'-'*12} {'-'*6} {'-'*10} {'-'*8} {'-'*5}")
        for _, row in df.iterrows():
            sig = "YES" if row["significant_mann_whitney"] else "no"
            print(
                f"  {row['metric']:<25} {row['Valkey_median']:>12.4f} "
                f"{row['Redis 7.2_median']:>12.4f} {row['mann_whitney_U']:>6.1f} "
                f"{row['mann_whitney_p']:>10.4f} {row['rank_biserial_r']:>8.3f} {sig:>5}"
            )
    return df


def generate_latex_table(perf_df: pd.DataFrame, output_dir: Path):
    """Generate LaTeX table for performance results."""
    if perf_df.empty:
        return

    ops_df = perf_df[perf_df["metric"] == "ops_sec"].copy()
    if ops_df.empty:
        return

    lines = [
        r"\begin{table}[htbp]",
        r"\centering",
        r"\caption{Wyniki testu Manna-Whitneya U dla przepustowości (ops/sec).}",
        r"\label{tab:mann-whitney-ops}",
        r"\begin{adjustbox}{max width=\textwidth}",
        r"\begin{tabular}{llrrrrl}",
        r"\toprule",
        r"Profil & vCPU & $U$ & $p$ & $p_{\mathrm{adj}}$ & $r$ & Istotny? \\",
        r"\midrule",
    ]

    for _, row in ops_df.iterrows():
        config = f"{int(row['payload_kb'])} KB, {row['ratio']}"
        cpu = int(row["cpu"])
        sig = "Tak" if row.get("significant_after_fdr", False) else "Nie"
        lines.append(
            f"  {config} & {cpu} & {row['mann_whitney_U']:.1f} & "
            f"{row['mann_whitney_p']:.4f} & {row['mann_whitney_p_adjusted']:.4f} & "
            f"{row['rank_biserial_r']:.3f} & {sig} \\\\"
        )

    lines.extend([
        r"\bottomrule",
        r"\end{tabular}",
        r"\end{adjustbox}",
        r"\end{table}",
    ])

    outfile = output_dir / "mann_whitney_ops_table.tex"
    outfile.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n  LaTeX table saved to: {outfile}")


def main():
    parser = argparse.ArgumentParser(description="Statistical tests for benchmark data")
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path(__file__).parent / "benchmark_results",
        help="Path to benchmark_results directory",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).parent / "statistical_output",
        help="Directory for output CSV/LaTeX files",
    )
    args = parser.parse_args()

    results_dir = args.results_dir
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    if not results_dir.exists():
        print(f"ERROR: results directory not found: {results_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Results directory: {results_dir}")
    print(f"Output directory:  {output_dir}")
    print(f"Significance level: α = {ALPHA}")

    # Run all tests
    perf_df = test_performance(results_dir)
    failover_df = test_failover(results_dir)
    backup_df = test_backup_restore(results_dir)
    reshard_df = test_resharding(results_dir)
    cpu_res_df = test_cpu_resilience(results_dir)
    mem_res_df = test_memory_resilience(results_dir)
    split_df = test_split_brain(results_dir)

    # Save detailed results to CSV
    all_results = {
        "performance": perf_df,
        "failover": failover_df,
        "backup_restore": backup_df,
        "resharding": reshard_df,
        "cpu_resilience": cpu_res_df,
        "memory_resilience": mem_res_df,
        "split_brain": split_df,
    }

    for name, df in all_results.items():
        if not df.empty:
            csv_path = output_dir / f"{name}_tests.csv"
            df.to_csv(csv_path, index=False)

    # Generate LaTeX table for performance
    generate_latex_table(perf_df, output_dir)

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    total_tests = sum(len(df) for df in all_results.values() if not df.empty)
    total_sig = 0
    for df in all_results.values():
        if not df.empty:
            if "significant_after_fdr" in df.columns:
                total_sig += df["significant_after_fdr"].sum()
            else:
                total_sig += df["significant_mann_whitney"].sum()

    print(f"  Total statistical tests performed: {total_tests}")
    print(f"  Significant results: {int(total_sig)}")
    print(f"\n  Output files saved to: {output_dir}/")


if __name__ == "__main__":
    main()

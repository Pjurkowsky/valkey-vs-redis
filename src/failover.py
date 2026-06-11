import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.chart_markers import mark_test_window

matplotlib.use("Agg")

BASELINE_SECONDS = 25
DROP_THRESHOLD = 0.50
STARTUP_IGNORE_SECONDS = 3
ERROR_RESPONSE_RE = re.compile(r"handle error response:\s*(-[A-Z0-9_]+)")
TIMESTAMPED_LOG_RE = re.compile(r"^(\d{10,})(?:\.\d+)?\t(.*)$")
CONNECTION_DROPPED_RE = re.compile(r"\bconnection dropped\.", re.IGNORECASE)
CONNECTION_ERROR_RE = re.compile(r"\bConnection error:\s*(.+)$")
CHAOS_DURATION_RE = re.compile(r"^([0-9]+(?:\.[0-9]+)?)(ms|s|m|h)?$")
MEMTIER_PROGRESS_RE = re.compile(
    r"\[RUN #\d+\s+\d+%,\s*(\d+)\s+secs\].*?:\s*"
    r"(\d+)\s+ops,\s*([-0-9.]+)\s+\(avg:.*?\)\s+ops/sec,"
    r".*?,\s*([-0-9a-zA-Z.]+)\s+\(avg:.*?\)\s+msec latency"
)

TS_COLUMNS = [
    "second",
    "count",
    "avg_latency",
    "min_latency",
    "max_latency",
    "p50",
    "p95",
    "p99",
    "p999",
]


def parse_time_series(run_result: Dict[str, Any]) -> pd.DataFrame:
    """Extract per-second time series from a single RUN result's Totals block."""
    totals = run_result.get("Totals", {})
    ts_raw = totals.get("Time-Serie", {})

    rows = []
    for sec_str in sorted(ts_raw.keys(), key=int):
        entry = ts_raw[sec_str]
        rows.append({
            "second": int(sec_str),
            "count": entry.get("Count", 0),
            "avg_latency": entry.get("Average Latency", float("nan")),
            "min_latency": entry.get("Min Latency", float("nan")),
            "max_latency": entry.get("Max Latency", float("nan")),
            "p50": entry.get("p50.00", float("nan")),
            "p95": entry.get("p95.00", float("nan")),
            "p99": entry.get("p99.00", float("nan")),
            "p999": entry.get("p99.90", float("nan")),
        })

    return pd.DataFrame(rows, columns=TS_COLUMNS)


def parse_failover_client_time_series(run_result: Dict[str, Any]) -> pd.DataFrame:
    """Extract per-second success throughput from failover_client output."""
    rows = []
    for entry in run_result.get("per_second", []):
        latency = entry.get("latency_ms") or {}
        rows.append({
            "second": int(entry.get("second", 0)),
            "count": entry.get("success_ops_sec", entry.get("succeeded", 0)),
            "avg_latency": _none_to_nan(latency.get("avg")),
            "min_latency": _none_to_nan(latency.get("min")),
            "max_latency": _none_to_nan(latency.get("max")),
            "p50": _none_to_nan(latency.get("p50")),
            "p95": _none_to_nan(latency.get("p95")),
            "p99": _none_to_nan(latency.get("p99")),
            "p999": _none_to_nan(latency.get("p999")),
        })

    return pd.DataFrame(rows, columns=TS_COLUMNS)


def parse_failover_client_errors(run_result: Dict[str, Any]) -> Tuple[Dict[str, Any], pd.DataFrame]:
    """Extract client-visible failed operations from failover_client output."""
    totals = run_result.get("totals", {})
    error_types = Counter(totals.get("error_types") or {})
    error_rows = []
    error_seconds = []

    for entry in run_result.get("per_second", []):
        second = int(entry.get("second", 0))
        failed = int(entry.get("failed", entry.get("failed_ops_sec", 0)) or 0)
        per_second_errors = Counter(entry.get("error_types") or {})
        clusterdown = int(per_second_errors.get("ClusterDownError", 0))
        if failed:
            error_seconds.append(second)
        error_rows.append({
            "second": second,
            "failed_request_count": failed,
            "clusterdown_errors": clusterdown,
        })

    error_ts = pd.DataFrame(error_rows)
    total_failed = int(totals.get("failed", sum(error_types.values()) if error_types else 0) or 0)
    total_clusterdown = int(error_types.get("ClusterDownError", 0))
    total_connection = int(
        error_types.get("ConnectionError", 0)
        + error_types.get("TimeoutError", 0)
        + error_types.get("ConnectionResetError", 0)
    )

    return {
        "log_file": None,
        "failed_request_count": total_failed,
        "clusterdown_errors": total_clusterdown,
        "connection_errors": total_connection,
        "connection_dropped": 0,
        "error_response_types": ", ".join(
            f"{error_type}:{count}" for error_type, count in sorted(error_types.items())
        ),
        "first_error_second": min(error_seconds) if error_seconds else None,
        "last_error_second": max(error_seconds) if error_seconds else None,
    }, error_ts


def parse_log_time_series(log_path: Path) -> pd.DataFrame:
    """Extract an approximate per-second series from memtier progress logs.

    memtier can emit an empty JSON time-series when all worker threads restart
    after connection failures. The progress log still contains the observable
    client-side throughput, so use it as a fallback for failover detection.
    """
    if not log_path.exists():
        return pd.DataFrame(columns=TS_COLUMNS)

    rows_by_second: Dict[int, Dict[str, Any]] = {}
    offset = 0
    last_raw_second: Optional[int] = None

    with log_path.open(errors="replace") as fh:
        for line in fh:
            _, message = _parse_timestamped_log_line(line)
            match = MEMTIER_PROGRESS_RE.search(message)
            if not match:
                continue

            raw_second = int(match.group(1))
            cumulative_ops = int(match.group(2))
            ops_per_second = _safe_float(match.group(3))
            latency = _safe_float(match.group(4))

            if last_raw_second is not None and raw_second < last_raw_second:
                offset += last_raw_second + 1
            last_raw_second = raw_second

            second = offset + raw_second
            count = 0 if cumulative_ops == 0 else max(0.0, ops_per_second)
            current = rows_by_second.get(second)
            if current is not None and current["count"] >= count:
                continue

            rows_by_second[second] = {
                "second": second,
                "count": count,
                "avg_latency": latency,
                "min_latency": latency,
                "max_latency": latency,
                "p50": latency,
                "p95": latency,
                "p99": latency,
                "p999": latency,
            }

    rows = [rows_by_second[sec] for sec in sorted(rows_by_second)]
    return pd.DataFrame(rows, columns=TS_COLUMNS)


def _safe_float(value: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float("nan")
    return parsed if np.isfinite(parsed) else float("nan")


def _none_to_nan(value: Any) -> float:
    if value is None:
        return float("nan")
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float("nan")
    return parsed if np.isfinite(parsed) else float("nan")


def detect_failover(ts: pd.DataFrame, chaos_second: Optional[int] = None) -> Dict[str, Any]:
    """Analyse a single run's time series to detect the failover window.

    Returns a dict with failover metrics or None values if no failover detected.
    """
    if ts.empty:
        return _empty_result()

    baseline_ts = _baseline_window(ts, chaos_second)
    if len(baseline_ts) < 3:
        return _empty_result()

    baseline_ops = baseline_ts["count"].mean()
    baseline_p99 = baseline_ts["p99"].mean()

    if baseline_ops == 0:
        return _empty_result()

    ts = _trim_terminal_partial_bucket(ts, baseline_ops)
    threshold = baseline_ops * DROP_THRESHOLD
    min_detection_second = (
        max(0, chaos_second - 1)
        if chaos_second is not None
        else STARTUP_IGNORE_SECONDS
    )
    detection_ts = ts[ts["second"] >= min_detection_second]
    degraded = detection_ts[detection_ts["count"] < threshold]

    if degraded.empty:
        return {
            "failover_detected": False,
            "baseline_ops": baseline_ops,
            "baseline_p99": baseline_p99,
            "failover_start_s": None,
            "failover_end_s": None,
            "failover_duration_s": 0.0,
            "failover_duration_ms": 0.0,
            "ops_lost": 0.0,
            "peak_p99_during": baseline_p99,
            "peak_max_latency_during": ts["max_latency"].max(),
        }

    failover_start = int(degraded["second"].min())
    failover_end = int(degraded["second"].max())
    duration_s = failover_end - failover_start + 1

    window = ts[(ts["second"] >= failover_start) & (ts["second"] <= failover_end)]
    ops_lost = max(0, baseline_ops * duration_s - window["count"].sum())
    peak_p99 = window["p99"].max()
    peak_max_lat = window["max_latency"].max()

    return {
        "failover_detected": True,
        "baseline_ops": baseline_ops,
        "baseline_p99": baseline_p99,
        "failover_start_s": failover_start,
        "failover_end_s": failover_end,
        "failover_duration_s": float(duration_s),
        "failover_duration_ms": float(duration_s * 1000),
        "ops_lost": ops_lost,
        "peak_p99_during": peak_p99,
        "peak_max_latency_during": peak_max_lat,
    }


def _baseline_window(ts: pd.DataFrame, chaos_second: Optional[int]) -> pd.DataFrame:
    if chaos_second is not None:
        baseline = ts[
            (ts["second"] >= STARTUP_IGNORE_SECONDS)
            & (ts["second"] < max(STARTUP_IGNORE_SECONDS + 1, chaos_second - 1))
        ]
        if len(baseline) >= 3:
            return baseline

    available_baseline = min(BASELINE_SECONDS, len(ts) // 2)
    return ts[
        (ts["second"] >= STARTUP_IGNORE_SECONDS)
        & (ts["second"] < max(STARTUP_IGNORE_SECONDS + 1, available_baseline))
    ]


def parse_memtier_error_log(
    log_path: Optional[Path],
    run_start_ms: Optional[float] = None,
) -> Tuple[Dict[str, Any], pd.DataFrame]:
    """Count client-visible errors printed by memtier."""
    if log_path is None or not log_path.exists():
        return {
            "log_file": None,
            "failed_request_count": None,
            "clusterdown_errors": None,
            "error_response_types": None,
            "first_error_second": None,
            "last_error_second": None,
        }, pd.DataFrame()

    error_types: Counter[str] = Counter()
    errors_by_second: Counter[int] = Counter()
    clusterdown_by_second: Counter[int] = Counter()
    error_seconds = []

    with log_path.open(errors="replace") as fh:
        for line in fh:
            timestamp_s, message = _parse_timestamped_log_line(line)
            match = ERROR_RESPONSE_RE.search(message)
            connection_error = CONNECTION_ERROR_RE.search(message)
            if match:
                error_type = match.group(1)
            elif CONNECTION_DROPPED_RE.search(message):
                error_type = "CONNECTION_DROPPED"
            elif connection_error:
                error_type = "CONNECTION_ERROR"
            else:
                continue

            error_types[error_type] += 1

            if timestamp_s is not None and run_start_ms is not None:
                second = max(0, int(timestamp_s - (run_start_ms / 1000.0)))
                errors_by_second[second] += 1
                if error_type == "-CLUSTERDOWN":
                    clusterdown_by_second[second] += 1
                error_seconds.append(second)

    error_ts = pd.DataFrame({
        "second": sorted(errors_by_second.keys()),
    })
    if not error_ts.empty:
        error_ts["failed_request_count"] = error_ts["second"].map(errors_by_second).astype(int)
        error_ts["clusterdown_errors"] = error_ts["second"].map(clusterdown_by_second).fillna(0).astype(int)

    return {
        "log_file": log_path.name,
        "failed_request_count": int(sum(error_types.values())),
        "clusterdown_errors": int(error_types.get("-CLUSTERDOWN", 0)),
        "connection_errors": int(error_types.get("CONNECTION_ERROR", 0)),
        "connection_dropped": int(error_types.get("CONNECTION_DROPPED", 0)),
        "error_response_types": ", ".join(
            f"{error_type}:{count}" for error_type, count in sorted(error_types.items())
        ),
        "first_error_second": min(error_seconds) if error_seconds else None,
        "last_error_second": max(error_seconds) if error_seconds else None,
    }, error_ts


def _parse_timestamped_log_line(line: str) -> Tuple[Optional[float], str]:
    match = TIMESTAMPED_LOG_RE.match(line.rstrip("\n"))
    if not match:
        return None, line

    return float(match.group(1)), match.group(2)


def _run_start_ms(run_result: Dict[str, Any]) -> Optional[float]:
    value = run_result.get("Runtime", {}).get("Start time")
    if value is None:
        return None
    return float(value)


def parse_failover_timing(timing_path: Path, run_start_ms: Optional[float]) -> Dict[str, Any]:
    if not timing_path.exists():
        return {
            "timing_file": None,
            "memtier_detected_start_second": None,
            "chaos_second": None,
            "chaos_duration_s": None,
            "steady_state_wait_s": None,
            "chaos_target": None,
        }

    with timing_path.open() as fh:
        timing = json.load(fh)

    chaos_epoch_s = timing.get("chaos_epoch_s")
    workload_started_epoch_s = timing.get("workload_started_epoch_s")
    memtier_started_epoch_s = timing.get("memtier_started_epoch_s")
    chaos_second = None
    memtier_detected_start_second = None
    relative_start_s = (
        float(workload_started_epoch_s or memtier_started_epoch_s)
        if (workload_started_epoch_s is not None or memtier_started_epoch_s is not None)
        else (run_start_ms / 1000.0 if run_start_ms is not None else None)
    )
    if chaos_epoch_s is not None and relative_start_s is not None:
        chaos_second = max(0, int(float(chaos_epoch_s) - relative_start_s))
    if (workload_started_epoch_s is not None or memtier_started_epoch_s is not None) and run_start_ms is not None:
        memtier_detected_start_second = int(
            float(workload_started_epoch_s or memtier_started_epoch_s) - (run_start_ms / 1000.0)
        )

    return {
        "timing_file": timing_path.name,
        "client_engine": timing.get("client_engine"),
        "memtier_detected_start_second": memtier_detected_start_second,
        "chaos_second": chaos_second,
        "chaos_duration_s": _duration_to_seconds(timing.get("duration")),
        "steady_state_wait_s": timing.get("steady_state_wait_s"),
        "chaos_target": timing.get("target"),
        "grace_period_s": timing.get("grace_period_s"),
        "relative_start_ms": relative_start_s * 1000.0 if relative_start_s is not None else None,
    }


def _duration_to_seconds(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    match = CHAOS_DURATION_RE.match(str(value).strip())
    if not match:
        return None

    amount = float(match.group(1))
    unit = match.group(2) or "s"
    if unit == "ms":
        return amount / 1000.0
    if unit == "m":
        return amount * 60.0
    if unit == "h":
        return amount * 3600.0
    return amount


def compare_baseline_to_chaos_window(
    ts: pd.DataFrame,
    timing_metrics: Dict[str, Any],
) -> Dict[str, Any]:
    """Compare the steady-state window before chaos with the chaos duration.

    The failover detector looks for the worst degraded window. For the thesis
    comparison we also want a simpler, fixed-window view: average ops/sec in
    the configured steady-state period before chaos and average ops/sec while
    Chaos Mesh keeps the master unavailable.
    """
    chaos_second = timing_metrics.get("chaos_second")
    if ts.empty or chaos_second is None:
        return _empty_window_comparison()

    steady_state_wait = timing_metrics.get("steady_state_wait_s") or BASELINE_SECONDS
    chaos_duration = timing_metrics.get("chaos_duration_s")

    baseline_start = max(0, int(float(chaos_second) - float(steady_state_wait)))
    baseline_end = int(float(chaos_second)) - 1
    baseline_ts = ts[(ts["second"] >= baseline_start) & (ts["second"] <= baseline_end)]

    if chaos_duration is None or chaos_duration <= 0:
        chaos_duration = max(0, int(ts["second"].max()) - int(float(chaos_second)) + 1)

    chaos_start = int(float(chaos_second))
    chaos_expected_s = max(0, int(round(float(chaos_duration))))
    chaos_end = chaos_start + chaos_expected_s - 1
    chaos_ts = ts[(ts["second"] >= chaos_start) & (ts["second"] <= chaos_end)]

    baseline_ops = _mean_or_none(baseline_ts["count"])
    chaos_observed_ops = _mean_or_none(chaos_ts["count"])
    chaos_expected_ops = _window_mean_with_missing_as_zero(
        chaos_ts,
        chaos_start,
        chaos_expected_s,
    )

    return {
        "baseline_window_start_s": baseline_start,
        "baseline_window_end_s": baseline_end,
        "baseline_window_expected_s": max(0, baseline_end - baseline_start + 1),
        "baseline_window_observed_s": int(len(baseline_ts)),
        "baseline_window_ops_sec": baseline_ops,
        "chaos_window_start_s": chaos_start,
        "chaos_window_end_s": chaos_end,
        "chaos_window_expected_s": chaos_expected_s,
        "chaos_window_observed_s": int(len(chaos_ts)),
        "chaos_window_ops_sec_observed": chaos_observed_ops,
        "chaos_window_ops_sec_missing_as_zero": chaos_expected_ops,
        "chaos_window_coverage_pct": (
            len(chaos_ts) / chaos_expected_s * 100.0
            if chaos_expected_s > 0 else None
        ),
        "chaos_window_drop_pct_observed": _drop_pct(baseline_ops, chaos_observed_ops),
        "chaos_window_drop_pct_missing_as_zero": _drop_pct(baseline_ops, chaos_expected_ops),
    }


def _empty_window_comparison() -> Dict[str, Any]:
    return {
        "baseline_window_start_s": None,
        "baseline_window_end_s": None,
        "baseline_window_expected_s": None,
        "baseline_window_observed_s": None,
        "baseline_window_ops_sec": None,
        "chaos_window_start_s": None,
        "chaos_window_end_s": None,
        "chaos_window_expected_s": None,
        "chaos_window_observed_s": None,
        "chaos_window_ops_sec_observed": None,
        "chaos_window_ops_sec_missing_as_zero": None,
        "chaos_window_coverage_pct": None,
        "chaos_window_drop_pct_observed": None,
        "chaos_window_drop_pct_missing_as_zero": None,
    }


def _mean_or_none(series: pd.Series) -> Optional[float]:
    if series.empty:
        return None
    return float(series.mean())


def _window_mean_with_missing_as_zero(
    ts: pd.DataFrame,
    start_second: int,
    expected_seconds: int,
) -> Optional[float]:
    if expected_seconds <= 0:
        return None
    if ts.empty:
        return 0.0

    counts = ts.set_index("second")["count"]
    window_index = range(start_second, start_second + expected_seconds)
    return float(counts.reindex(window_index, fill_value=0).mean())


def _drop_pct(baseline: Optional[float], measured: Optional[float]) -> Optional[float]:
    if baseline is None or measured is None or baseline == 0:
        return None
    return max(0.0, (baseline - measured) / baseline * 100.0)


def _trim_terminal_partial_bucket(ts: pd.DataFrame, baseline_ops: float) -> pd.DataFrame:
    """Ignore the final short bucket emitted when memtier exits."""
    if len(ts) < 2:
        return ts

    last = ts.iloc[-1]
    prev = ts.iloc[-2]
    threshold = baseline_ops * DROP_THRESHOLD

    if last["count"] < threshold and prev["count"] >= threshold:
        return ts.iloc[:-1].copy()

    return ts


def _empty_result() -> Dict[str, Any]:
    return {
        "failover_detected": False,
        "baseline_ops": None,
        "baseline_p99": None,
        "failover_start_s": None,
        "failover_end_s": None,
        "failover_duration_s": None,
        "failover_duration_ms": None,
        "ops_lost": None,
        "peak_p99_during": None,
        "peak_max_latency_during": None,
    }


def analyse_failover_runs(json_dir: Path) -> Tuple[pd.DataFrame, List[pd.DataFrame]]:
    """Parse all failover_run_*.json files and return (summary_df, list_of_timeseries)."""
    files = sorted(json_dir.glob("failover_run_*.json"))
    if not files:
        raise FileNotFoundError(f"No failover_run_*.json files in {json_dir}")

    results = []
    all_ts = []

    for f in files:
        with f.open() as fh:
            doc = json.load(fh)

        if doc.get("benchmark") == "failover_client":
            run_result = doc
            started_epoch_s = doc.get("run", {}).get("started_epoch_s")
            run_start_ms = float(started_epoch_s) * 1000.0 if started_epoch_s is not None else None
            timing_metrics = parse_failover_timing(_timing_path_for_run(f), run_start_ms)
            ts = parse_failover_client_time_series(run_result)
            error_metrics, error_ts = parse_failover_client_errors(run_result)
        else:
            run_result = doc.get("RUN #1 RESULTS") or doc.get("ALL STATS")
            if run_result is None:
                for key in doc:
                    if key.startswith("RUN #") and key.endswith("RESULTS"):
                        run_result = doc[key]
                        break

            if run_result is None:
                print(f"  [warn] No run results found in {f.name}, skipping")
                continue

            run_start_ms = _run_start_ms(run_result)
            timing_metrics = parse_failover_timing(_timing_path_for_run(f), run_start_ms)

            ts = parse_time_series(run_result)
            if ts.empty:
                ts = parse_log_time_series(f.with_suffix(".log"))

            relative_start_ms = timing_metrics.get("relative_start_ms") or run_start_ms
            error_metrics, error_ts = parse_memtier_error_log(f.with_suffix(".log"), relative_start_ms)

        metrics = detect_failover(ts, timing_metrics.get("chaos_second"))
        metrics.update(timing_metrics)
        metrics.update(compare_baseline_to_chaos_window(ts, timing_metrics))
        metrics.update(error_metrics)
        metrics.update(_compute_error_window(error_metrics, timing_metrics))
        metrics["file"] = f.name

        ts = _merge_error_series(ts, error_ts)
        all_ts.append(ts)
        results.append(metrics)

    return pd.DataFrame(results), all_ts


def _compute_error_window(
    error_metrics: Dict[str, Any],
    timing_metrics: Dict[str, Any],
) -> Dict[str, Any]:
    """Derive promotion-time metrics from the client-visible error window.

    The error window (first_error_second .. last_error_second) captures the
    period during which the client observed failures reaching the affected
    shard.  Its duration is independent of the PodChaos lifetime and
    approximates: cluster-node-timeout + replica election + client reconnection.
    """
    first_err = error_metrics.get("first_error_second")
    last_err = error_metrics.get("last_error_second")
    chaos_second = timing_metrics.get("chaos_second")

    if first_err is not None and last_err is not None:
        ew = int(last_err) - int(first_err) + 1
    else:
        ew = None

    if first_err is not None and chaos_second is not None:
        delay = int(first_err) - int(chaos_second)
    else:
        delay = None

    return {
        "error_window_duration_s": float(ew) if ew is not None else None,
        "error_window_duration_ms": float(ew * 1000) if ew is not None else None,
        "first_error_delay_s": float(delay) if delay is not None else None,
    }


def _timing_path_for_run(run_path: Path) -> Path:
    return run_path.with_name(run_path.name.replace("failover_run_", "failover_timing_", 1))


def _merge_error_series(ts: pd.DataFrame, error_ts: pd.DataFrame) -> pd.DataFrame:
    ts = ts.copy()
    if error_ts.empty:
        ts["failed_request_count"] = 0
        ts["clusterdown_errors"] = 0
        return ts

    ts = ts.merge(error_ts, on="second", how="left")
    ts["failed_request_count"] = ts["failed_request_count"].fillna(0).astype(int)
    ts["clusterdown_errors"] = ts["clusterdown_errors"].fillna(0).astype(int)
    return ts


def print_failover_summary(df: pd.DataFrame) -> None:
    """Print aggregated failover statistics."""
    detected = df[df["failover_detected"] == True]

    print(f"\nFailover runs analysed: {len(df)}")
    print(f"Failover detected in:  {len(detected)}/{len(df)} runs")

    detection_metrics = [
        ("Failover duration (ms)", "failover_duration_ms"),
        ("Error window duration (ms)", "error_window_duration_ms"),
        ("First error delay (s)", "first_error_delay_s"),
        ("Ops lost", "ops_lost"),
        ("Detected baseline ops/sec", "baseline_ops"),
        ("Peak p99 during failover (ms)", "peak_p99_during"),
        ("Detected baseline p99 (ms)", "baseline_p99"),
    ]
    window_metrics = [
        ("Baseline window ops/sec", "baseline_window_ops_sec"),
        ("Chaos window ops/sec (observed)", "chaos_window_ops_sec_observed"),
        ("Chaos window ops/sec (missing=0)", "chaos_window_ops_sec_missing_as_zero"),
        ("Chaos window drop % (observed)", "chaos_window_drop_pct_observed"),
        ("Chaos window drop % (missing=0)", "chaos_window_drop_pct_missing_as_zero"),
        ("Chaos window coverage (%)", "chaos_window_coverage_pct"),
    ]

    if detected.empty:
        print("No degraded failover window detected by threshold.")
    else:
        _print_metric_table("Detected failover window", detected, detection_metrics)

    _print_metric_table("Configured baseline vs chaos window", df, window_metrics)
    _print_memtier_error_summary(df)


def _print_metric_table(
    title: str,
    df: pd.DataFrame,
    metrics: List[Tuple[str, str]],
) -> None:
    available = [(label, col) for label, col in metrics if col in df and df[col].notna().any()]
    if not available:
        return

    print(f"\n{title}:")
    print(f"\n{'Metric':<40} {'Mean':>12} {'Std':>12}")
    print("-" * 66)
    for label, col in available:
        mean = df[col].mean()
        std = df[col].std()
        print(f"{label:<40} {mean:>12.2f} {std:>12.2f}")


def _print_memtier_error_summary(df: pd.DataFrame) -> None:
    error_df = df[df["failed_request_count"].notna()] if "failed_request_count" in df else pd.DataFrame()
    if not error_df.empty:
        runs_with_errors = int((error_df["failed_request_count"] > 0).sum())
        total_failed = int(error_df["failed_request_count"].sum())
        total_clusterdown = int(error_df["clusterdown_errors"].sum())
        total_connection_errors = int(error_df.get("connection_errors", pd.Series(dtype=int)).sum())
        total_connection_dropped = int(error_df.get("connection_dropped", pd.Series(dtype=int)).sum())
        print("\nClient-visible errors:")
        print(f"  Runs with errors:       {runs_with_errors}/{len(error_df)}")
        print(f"  Error events total:     {total_failed}")
        print(f"  CLUSTERDOWN responses:  {total_clusterdown}")
        print(f"  Connection errors:      {total_connection_errors}")
        print(f"  Connection dropped:     {total_connection_dropped}")


def save_failover_csv(df: pd.DataFrame, out_dir: Path) -> None:
    csv_path = out_dir / "failover_summary.csv"
    df.to_csv(csv_path, index=False)
    print(f"\nFailover CSV saved to {csv_path}")


def plot_failover_timeseries(
    all_ts: List[pd.DataFrame],
    results_df: pd.DataFrame,
    out_dir: Path,
) -> None:
    """Plot ops/sec over time for each failover run, highlighting the failover window."""
    for i, (ts, (_, row)) in enumerate(zip(all_ts, results_df.iterrows())):
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(10, 7.5), sharex=True)

        ax1.plot(ts["second"], ts["count"], linewidth=0.8, color="#4c72b0")
        ax1.set_ylabel("Ops/sec")
        ax1.set_title(f"Failover Run {i + 1}")

        has_failover_window = bool(row.get("failover_detected")) and _has_value(row.get("failover_start_s"))
        chaos_second = row.get("chaos_second")
        has_chaos_second = _has_value(chaos_second)

        if has_failover_window:
            ax1.axvspan(
                row["failover_start_s"], row["failover_end_s"],
                alpha=0.2, color="red", label="Failover window",
            )
            ax1.axhline(
                row["baseline_ops"], linestyle="--", color="gray",
                linewidth=0.7, label="Baseline",
            )

        if has_chaos_second:
            mark_test_window(
                ax1,
                chaos_second,
                None,
                with_labels=True,
                start_label="Chaos injected",
            )
        ax1.legend(fontsize=8)

        ax2.plot(ts["second"], ts["p99"], linewidth=0.8, color="#c44e52", label="p99")
        ax2.plot(ts["second"], ts["p50"], linewidth=0.8, color="#55a868", label="p50")
        ax2.set_ylabel("Latency (ms)")
        ax2.legend(fontsize=8)

        if has_failover_window:
            ax2.axvspan(
                row["failover_start_s"], row["failover_end_s"],
                alpha=0.2, color="red",
            )
        if has_chaos_second:
            mark_test_window(
                ax2,
                chaos_second,
                None,
                start_label="Chaos injected",
            )

        other_errors = (ts["failed_request_count"] - ts["clusterdown_errors"]).clip(lower=0)
        ax3.bar(
            ts["second"], ts["clusterdown_errors"],
            width=0.8, color="#8172b3", label="CLUSTERDOWN",
        )
        if other_errors.sum() > 0:
            ax3.bar(
                ts["second"], other_errors, bottom=ts["clusterdown_errors"],
                width=0.8, color="#ccb974", label="Other errors",
            )
        ax3.set_ylabel("Errors/sec")
        ax3.set_xlabel("Time (s)")
        if ts["failed_request_count"].sum() > 0:
            ax3.legend(fontsize=8)

        if has_failover_window:
            ax3.axvspan(
                row["failover_start_s"], row["failover_end_s"],
                alpha=0.2, color="red",
            )
        if has_chaos_second:
            mark_test_window(
                ax3,
                chaos_second,
                None,
                start_label="Chaos injected",
            )

        fig.tight_layout()
        fig.savefig(out_dir / f"failover_run_{i + 1}.png", dpi=150, bbox_inches="tight")
        plt.close(fig)


def _has_value(value: Any) -> bool:
    return value is not None and not pd.isna(value)


def _test_end_second(row: pd.Series, chaos_second: Any) -> Optional[float]:
    if not _has_value(chaos_second):
        return None

    failover_end = row.get("failover_end_s")
    if _has_value(failover_end):
        return max(float(chaos_second), float(failover_end))

    grace_period = row.get("grace_period_s")
    if _has_value(grace_period):
        return float(chaos_second) + float(grace_period)

    return float(chaos_second)


def plot_failover_comparison(results_df: pd.DataFrame, out_dir: Path) -> None:
    """Bar chart comparing failover duration across runs."""
    detected = results_df[results_df["failover_detected"] == True]
    if detected.empty:
        return

    fig, ax = plt.subplots(figsize=(6, 4))
    x = np.arange(len(detected))
    ax.bar(x, detected["failover_duration_ms"], color="#dd8452", edgecolor="black", linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels([f"Run {i+1}" for i in range(len(detected))])
    ax.set_ylabel("Failover Duration (ms)")
    ax.set_title("Failover Duration Across Runs")

    mean_val = detected["failover_duration_ms"].mean()
    ax.axhline(mean_val, linestyle="--", color="gray", linewidth=0.7,
               label=f"Mean: {mean_val:.0f} ms")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_dir / "failover_comparison.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

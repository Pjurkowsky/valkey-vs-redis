from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import numpy as np

from src.models import CPU_QUERY, MEM_QUERY, Metric, RunConfig


def build_prom(url: str):
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

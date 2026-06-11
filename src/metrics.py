import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np

from src.models import Metric, RunConfig


@dataclass
class MonitoringQueries:
    cpu: List[str]
    memory: List[str]
    restarts: List[str]


def build_prom(url: str):
    from prometheus_api_client import PrometheusConnect
    return PrometheusConnect(url=url, disable_ssl=True)


def _prom_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _prom_regex(value: str) -> str:
    # Kubernetes service/pod prefixes used here are DNS labels. Keeping hyphens
    # literal avoids over-escaping regexes such as redis72-redis-cluster-[0-9]+.
    return _prom_string(value)


def _namespace_from_host(host: str, default: str) -> str:
    parts = host.split(".")
    if len(parts) >= 2 and parts[2:4] == ["svc", "cluster"]:
        return parts[1]
    if len(parts) >= 2 and parts[2:3] == ["svc"]:
        return parts[1]
    return default


def _service_from_host(host: str, default: str) -> str:
    return host.split(".")[0] if host else default


def build_monitoring_queries(doc: Dict[str, Any]) -> MonitoringQueries:
    provider = str(doc.get("provider") or doc.get("variant") or "valkey").lower()
    target = doc.get("target") or {}
    host = str(target.get("host") or "")

    if provider in ("redis", "redis72"):
        service = _service_from_host(host, "redis72-redis-cluster")
        namespace = _namespace_from_host(host, "redis")
        pod_regex = f"{_prom_regex(service)}-[0-9]+"
        main_container = service
    else:
        release = _service_from_host(host, "valkey")
        namespace = _namespace_from_host(host, "vk")
        pod_regex = f"{_prom_regex(release)}-[0-9]+"
        main_container = "valkey"

    ns = _prom_string(namespace)
    pod = _prom_string(pod_regex)
    container = _prom_string(main_container)

    # First query the main data container. Fallbacks keep older result sets usable if
    # the container label differs or was not scraped with the expected name.
    cpu = [
        f'sum by (pod)(rate(container_cpu_usage_seconds_total{{namespace="{ns}",pod=~"{pod}",container="{container}"}}[30s]))',
        f'sum by (pod)(rate(container_cpu_usage_seconds_total{{namespace="{ns}",pod=~"{pod}"}}[30s]))',
        f'sum by (pod)(rate(container_cpu_usage_seconds_total{{namespace="{ns}",pod=~"{pod}",container!="",container!="POD"}}[30s]))',
        f'sum by (pod)(rate(container_cpu_usage_seconds_total{{pod=~"{pod}",container="{container}"}}[30s]))',
        f'sum by (pod)(rate(container_cpu_usage_seconds_total{{pod=~"{pod}"}}[30s]))',
    ]
    memory = [
        f'sum by (pod)(container_memory_working_set_bytes{{namespace="{ns}",pod=~"{pod}",container="{container}"}})',
        f'sum by (pod)(container_memory_working_set_bytes{{namespace="{ns}",pod=~"{pod}"}})',
        f'sum by (pod)(container_memory_working_set_bytes{{namespace="{ns}",pod=~"{pod}",container!="",container!="POD"}})',
        f'sum by (pod)(container_memory_working_set_bytes{{pod=~"{pod}",container="{container}"}})',
        f'sum by (pod)(container_memory_working_set_bytes{{pod=~"{pod}"}})',
    ]
    restarts = [
        f'kube_pod_container_status_restarts_total{{namespace="{ns}",pod=~"{pod}",container="{container}"}}',
        f'kube_pod_container_status_restarts_total{{namespace="{ns}",pod=~"{pod}"}}',
        f'kube_pod_container_status_restarts_total{{namespace="{ns}",pod=~"{pod}",container!="",container!="POD"}}',
        f'kube_pod_container_status_restarts_total{{pod=~"{pod}",container="{container}"}}',
        f'kube_pod_container_status_restarts_total{{pod=~"{pod}"}}',
    ]
    return MonitoringQueries(cpu=cpu, memory=memory, restarts=restarts)


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


def first_mean_from_range(
    prom,
    queries: Iterable[str],
    start: datetime,
    end: datetime,
) -> Dict[str, float]:
    for query in queries:
        means = mean_from_range(prom, query, start, end)
        if means:
            return means
    return {}


def counter_increase_by_pod(prom, queries: Iterable[str], start: datetime, end: datetime) -> Dict[str, float]:
    for query in queries:
        data = prom.custom_query_range(
            query=query,
            start_time=start,
            end_time=end,
            step="15s",
        )
        increases: Dict[str, float] = {}
        for series in data:
            label = series["metric"].get("pod", "total")
            values = [float(v[1]) for v in series["values"]]
            if not values:
                continue
            increases[label] = max(values[-1] - values[0], 0.0)
        if increases:
            return increases
    return {}


def parse_filename(filename: Path) -> RunConfig:
    cpu, payload, ratio = filename.stem.split("_")
    return RunConfig(int(cpu), int(payload), ratio)


def extract_metric(
    run_result: Dict[str, Any],
    prom: Optional[Any] = None,
    queries: Optional[MonitoringQueries] = None,
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
    pod_restarts = float("nan")

    if prom is not None and queries is not None and start_ms and finish_ms:
        start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(finish_ms / 1000, tz=timezone.utc)
        try:
            mem_mean = first_mean_from_range(prom, queries.memory, start_dt, end_dt)
            if mem_mean:
                memory_usage = sum(v for v in mem_mean.values() if v is not None)
        except Exception as e:
            print(f"  [warn] memory query failed: {e}")

        try:
            cpu_mean = first_mean_from_range(prom, queries.cpu, start_dt, end_dt)
            if cpu_mean:
                cpu_util = sum(v for v in cpu_mean.values() if v is not None)
        except Exception as e:
            print(f"  [warn] cpu query failed: {e}")

        try:
            restart_increase = counter_increase_by_pod(prom, queries.restarts, start_dt, end_dt)
            if restart_increase:
                pod_restarts = sum(v for v in restart_increase.values() if v is not None)
        except Exception as e:
            print(f"  [warn] restart query failed: {e}")

    return Metric(ops_sec, p50, p95, p99, p999, cpu_util, memory_usage, pod_restarts)


def extract_cpu_util_by_pod(
    run_result: Dict[str, Any],
    prom: Optional[Any] = None,
    queries: Optional[MonitoringQueries] = None,
) -> Dict[str, float]:
    runtime = run_result.get("Runtime", {})
    start_ms = runtime.get("Start time")
    finish_ms = runtime.get("Finish time")

    if prom is None or queries is None or not start_ms or not finish_ms:
        return {}

    start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(finish_ms / 1000, tz=timezone.utc)
    try:
        return first_mean_from_range(prom, queries.cpu, start_dt, end_dt)
    except Exception as e:
        print(f"  [warn] per-pod cpu query failed: {e}")
        return {}


def extract_memory_usage_by_pod(
    run_result: Dict[str, Any],
    prom: Optional[Any] = None,
    queries: Optional[MonitoringQueries] = None,
) -> Dict[str, float]:
    runtime = run_result.get("Runtime", {})
    start_ms = runtime.get("Start time")
    finish_ms = runtime.get("Finish time")

    if prom is None or queries is None or not start_ms or not finish_ms:
        return {}

    start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(finish_ms / 1000, tz=timezone.utc)
    try:
        return first_mean_from_range(prom, queries.memory, start_dt, end_dt)
    except Exception as e:
        print(f"  [warn] per-pod memory query failed: {e}")
        return {}


def iter_run_results(doc: Dict[str, Any]) -> Iterable[Tuple[int, Dict[str, Any]]]:
    runs = []
    for key, value in doc.items():
        match = re.fullmatch(r"RUN #(\d+) RESULTS", key)
        if match and isinstance(value, dict):
            runs.append((int(match.group(1)), value))

    if runs:
        yield from sorted(runs)
    elif isinstance(doc.get("ALL STATS"), dict):
        yield 1, doc["ALL STATS"]

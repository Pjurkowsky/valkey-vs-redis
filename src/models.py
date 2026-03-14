from dataclasses import dataclass

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


def ratio_label(r: str) -> str:
    return RATIO_LABELS.get(r, r)

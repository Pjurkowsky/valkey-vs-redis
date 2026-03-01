from pathlib import Path
import argparse
import json
from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

from prometheus_api_client import PrometheusConnect
from datetime import datetime, timezone

prom = PrometheusConnect(url="http://localhost:9090", disable_ssl=True)

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

mem_query = """
sum by (pod)(
  container_memory_working_set_bytes{
    pod=~"valkey-[0-5]"
  }
)
"""

def mean_from_range(query, start, end):
    data = prom.custom_query_range(
        query=query,
        start_time=start,
        end_time=end,
        step="1s"  
    )

    means = {}
    for series in data:
        label = series["metric"].get("pod", "total")
        values = [float(v[1]) for v in series["values"]]
        means[label] = np.mean(values) if values else None

    return means   

def parse_filename(filename: Path)->RunConfig:
    cpu, payload, ratio = filename.stem.split("_")
    return RunConfig(cpu, payload, ratio)

def extract_metric(run_result: Dict[str, Any]) ->Metric:
    runtime = run_result.get("Runtime", {})
    start_ms = runtime.get("Start time")
    finish_ms = runtime.get("Finish time")

    print(start_ms, finish_ms)
    totals = run_result.get("Totals", {})
    ops_sec = totals.get("Ops/sec")

    precentile_block = totals.get("Percentile Latencies", {})
    p50 = precentile_block.get("p50.00")
    p95 = precentile_block.get("p95.00")
    p99 = precentile_block.get("p99.00")
    p999 = precentile_block.get("p99.90")

    start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    end_dt   = datetime.fromtimestamp(finish_ms / 1000, tz=timezone.utc)

    mem_mean = mean_from_range(mem_query, start_dt, end_dt)
    print("Memory mean (bytes):", mem_mean)

    return Metric(
        ops_sec,
        p50,
        p95,
        p99,
        p999,
        0,
        0
    )


def iter_run_results(doc: Dict[str, Any]) -> Iterable[Tuple[int, Dict[str, Any]]]:
    for i in range(1, 6):
        key = f"RUN #{i} RESULTS"
        if key in doc and isinstance(doc[key], dict):
            yield i, doc[key]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input")
    args = ap.parse_args()
    
    directory = Path(args.input)
    rows = []
    for file in directory.glob("*.json"):
        config = parse_filename(file)

        with file.open("r") as f:
            doc = json.load(f)    

        for run_id, run_result in iter_run_results(doc):
            metrics = extract_metric(run_result)
            rows.append({
                "id": run_id,
                **asdict(config),
                **asdict(metrics),
            })
    print(rows)

    df = pd.DataFrame(rows)
    metrics = ["ops_sec", "p50", "p95", "p99", "p999", "cpu_util", "memory_usage"]

    agg = (
        df.groupby(["cpu", "payload", "ratio"], as_index=False)
        .agg(
            n=("id", "count"),
            **{f"{m}_mean": (m, "mean") for m in metrics},
            **{f"{m}_std":  (m, "std")  for m in metrics},
        )
    )

    print(df.head())
    print(agg.head())

    for ratio in sorted(df["ratio"].unique()):
        sub = agg[agg["ratio"] == ratio]
        for payload in sorted(sub["payload"].unique()):
            s2 = sub[sub["payload"] == payload].sort_values("cpu")
            plt.figure()
            plt.plot(s2["cpu"], s2["ops_sec_mean"])
            plt.title(f"Ops/sec mean vs vCPU | ratio={ratio} | payload={payload}")
            plt.xlabel("vCPU")
            plt.ylabel("Ops/sec (mean)")
            plt.tight_layout()
            plt.show()

if __name__ == "__main__":
    main()
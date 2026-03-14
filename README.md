# Valkey vs. Redis: Performance and Reliability Comparison of In-Memory Cache Clusters in Cloud-Native Architectures

---

### Currently working on:

[https://github.com/valkey-io/valkey-helm/pull/116](https://github.com/valkey-io/valkey-helm/pull/116)

---

## Stack Overview

- Kubernetes
- Valkey Helm Chart
- Prometheus + Grafana (kube-prometheus-stack)
- ServiceMonitor enabled for metrics scraping
- memtier_benchmark (run as a pod via custom Docker image)

Valkey deployed in cluster mode:

- 3 shards
- 1 replica per shard
- Persistence enabled (5Gi)
- maxmemory (1Gi)

## Benchmark Configuration


| Variable        | Values                                               |
| --------------- | ---------------------------------------------------- |
| vCPU            | 1, 2, 4                                              |
| Payload size    | 1 KB, 10 KB, 1000 KB                                 |
| Operation ratio | read-only (0:1), write-only (1:0), mixed 50/50 (1:1) |
| Repetitions     | N=5 per configuration                                |


Metrics collected: ops/sec, latency distribution (p50, p95, p99, p99.9), CPU utilization, memory usage.

## Deployment

### Install Prometheus and Grafana

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

helm install monitoring prometheus-community/kube-prometheus-stack -n monitoring --create-namespace
```

### Port-forward Prometheus

```bash
kubectl -n monitoring port-forward svc/monitoring-kube-prometheus-prometheus 9090:9090
```

### Grafana password

```bash
kubectl get secret -n monitoring monitoring-grafana -o jsonpath="{.data.admin-password}" | base64 -d; echo
```

### Import Grafana dashboard

1. Click **+ → Import**
2. Enter dashboard ID: `763`
3. Click Load
4. Select Prometheus datasource
5. Import

### Install Valkey

```bash
git clone https://github.com/valkey-io/valkey-helm.git
cd valkey-helm

git fetch origin pull/116/head:pr-116
git checkout pr-116
```

```bash
helm install valkey ../valkey-helm/valkey -n vk -f ./k8s/values.yaml --create-namespace
```

### Deploy StatsD exporter

```bash
kubectl apply -f ./k8s/statsd-exporter.yaml
kubectl apply -f ./k8s/statsd-exporter-servicemonitor.yaml
```

### Build and run benchmark

```bash
docker build -t memtier_k8s:1 ./k8s
minikube image load memtier_k8s:1
```

Run the full benchmark and copy results to the host:

```bash
./k8s/run_benchmark.sh ./results_memtier
```

This launches a pod, executes all benchmark configurations, copies the JSON results back to `./results_memtier/`, and cleans up the pod.

To use a different image, set `MEMTIER_IMAGE`:

```bash
MEMTIER_IMAGE=memtier_k8s:2 ./k8s/run_benchmark.sh ./results_memtier
```

### Analyse results

```bash
# With Prometheus (CPU/memory metrics)
python main.py --input ./results_memtier --output-dir ./benchmark_plots

# Without Prometheus (JSON-only metrics)
python main.py --input ./test_data --no-prometheus --output-dir ./benchmark_plots
```

Plots and `summary.csv` are saved to the output directory.
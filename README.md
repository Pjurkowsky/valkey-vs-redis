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
| Client threads  | 4                                                    |
| Clients/thread  | 25                                                   |
| Pipeline        | 10                                                   |
| Test time       | 300s per run                                        |
| Keyspace        | 1,000,000 keys                                      |


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

### Port-forward Grafana

```bash
kubectl -n monitoring port-forward svc/monitoring-grafana 3000:80
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
helm install valkey ../valkey-helm/valkey -n vk -f ./k8s/manifests/values.yaml --create-namespace
```

### Deploy StatsD exporter

```bash
kubectl apply -f ./k8s/manifests/statsd-exporter.yaml
kubectl apply -f ./k8s/manifests/statsd-exporter-servicemonitor.yaml
```

### Build and run benchmark

```bash
docker build -t memtier_k8s:1 ./k8s/images/memtier
minikube image load memtier_k8s:1
```

Grant the benchmark pod permission to update and observe the Valkey StatefulSet during CPU sweep runs:

```bash
kubectl apply -f ./k8s/memtier_rbac.yaml
```

Run the full benchmark and copy results to the host:

```bash
./k8s/scripts/run_benchmark.sh ./results/memtier
```

This launches a pod with the `memtier-sa` service account, executes all benchmark configurations, copies the JSON results back to `./results/memtier/`, and cleans up the pod.

To use a different image, set `MEMTIER_IMAGE`:

```bash
MEMTIER_IMAGE=memtier_k8s:2 ./k8s/scripts/run_benchmark.sh ./results/memtier
```

### Persistence and TLS benchmark variants

`run_benchmark.sh` can optionally apply Valkey runtime variants before creating the benchmark pod.

Persistence controls Valkey RDB/AOF behavior in `valkey.conf`:

```bash
# No RDB snapshots, no AOF
VALKEY_PERSISTENCE=off ./k8s/scripts/run_benchmark.sh ./results/memtier_no_persistence

# RDB snapshots only
VALKEY_PERSISTENCE=rdb ./k8s/scripts/run_benchmark.sh ./results/memtier_rdb

# AOF only
VALKEY_PERSISTENCE=aof ./k8s/scripts/run_benchmark.sh ./results/memtier_aof

# RDB + AOF
VALKEY_PERSISTENCE=both ./k8s/scripts/run_benchmark.sh ./results/memtier_persistence
```

Supported values:

```text
off, rdb, aof, both
```

In cluster mode the Helm chart still creates PVCs for the pods. The flag above controls whether
Valkey writes RDB/AOF persistence data, not whether the StatefulSet has volumes.

To enable TLS, first create a Kubernetes secret with `server.crt`, `server.key`, and `ca.crt`:

```bash
kubectl create secret generic valkey-tls-secret -n vk \
  --from-file=server.crt \
  --from-file=server.key \
  --from-file=ca.crt
```

Then run the benchmark with server-side TLS and memtier TLS enabled:

```bash
VALKEY_TLS=true \
VALKEY_TLS_SECRET=valkey-tls-secret \
MEMTIER_TLS=true \
MEMTIER_TLS_SKIP_VERIFY=true \
./k8s/scripts/run_benchmark.sh ./results/memtier_tls
```

To disable TLS again:

```bash
VALKEY_TLS=false ./k8s/scripts/run_benchmark.sh ./results/memtier_plain
```

For a production-like TLS test, prefer using a real CA and set `MEMTIER_TLS_SKIP_VERIFY=false`
with `MEMTIER_TLS_CACERT` pointing to the CA file inside the memtier image.

### Analyse results

```bash
# With Prometheus (CPU/memory metrics)
python cli.py benchmark --input ./results/memtier --output-dir ./plots/benchmark

# Without Prometheus (JSON-only metrics)
python cli.py benchmark --input ./test_data --no-prometheus --output-dir ./plots/benchmark

# Backwards-compatible shortcut (delegates to cli.py benchmark)
python main.py --input ./results/memtier --output-dir ./plots/benchmark
```

Plots and `summary.csv` are saved to the output directory.

## Failover Testing

Measures how long (in milliseconds and lost ops) it takes for Valkey to recover after a master pod is killed,
using [Chaos Mesh](https://chaos-mesh.org/) for fault injection.

### Install Chaos Mesh

```bash
./k8s/scripts/chaos-mesh-install.sh
```

### Run failover benchmark

Runs memtier_benchmark under sustained load, kills a Valkey master pod at ~30s, and captures
the per-second ops/latency time series plus a timestamped memtier stdout/stderr log. The script
waits until memtier's timed run has started, then waits for the steady-state interval before
injecting chaos. It runs once by default.

```bash
./k8s/scripts/failover_benchmark.sh ./results/failover
```

Override the number of runs or image:

```bash
N=3 MEMTIER_IMAGE=memtier_k8s:2 ./k8s/scripts/failover_benchmark.sh ./results/failover
```

### Analyse failover results

```bash
python cli.py failover --input ./results/failover --output-dir ./plots/failover
```

Produces per-run time series plots (ops/sec and latency with failover window highlighted),
a comparison bar chart, and `failover_summary.csv` with mean +/- std for:
failover duration (ms), ops lost, baseline ops/sec, peak p99 during failover, and failed
responses seen in the memtier logs such as `-CLUSTERDOWN`. When logs include timestamps,
the per-run plot also shows failed responses per second. New runs also write
`failover_timing_*.json`, which records when Chaos Mesh was applied so the plot can mark the
actual injection time separately from the detected impact window.

### Note on `cluster-node-timeout`

Failover time depends on the Valkey `cluster-node-timeout` setting (default: 15000ms).
The current `k8s/manifests/values.yaml` uses the default. To change it, add to `valkeyConfig`:

```
cluster-node-timeout 5000
```

## Resilience Testing

Measures how Valkey behaves under resource starvation (CPU throttling, memory pressure / OOM Kill)
using Chaos Mesh StressChaos experiments. Requires Chaos Mesh to be installed (see above).

### Run CPU stress test

Saturates CPU on one Valkey pod for 30s during sustained load:

```bash
./k8s/scripts/resilience_benchmark.sh cpu ./results/resilience
```

### Run memory stress test

Allocates 900 MB on one Valkey pod for 60s (against the 1 GB maxmemory limit):

```bash
./k8s/scripts/resilience_benchmark.sh memory ./results/resilience
```

### Run extreme memory stress test

Allocates 1800 MB per worker with 2 workers on one Valkey pod for 60s:

```bash
./k8s/scripts/resilience_benchmark.sh memory-extreme ./results/resilience
```

### Analyse resilience results

```bash
python cli.py resilience --input ./results/resilience --scenario cpu --output-dir ./plots/resilience
python cli.py resilience --input ./results/resilience --scenario memory --output-dir ./plots/resilience
python cli.py resilience --input ./results/resilience --scenario memory-extreme --output-dir ./plots/resilience
```

Produces per-run time series plots, comparison charts, and `resilience_{scenario}_summary.csv` with:
degradation duration, ops/sec drop (%), min ops/sec during stress, peak p99/p99.9,
recovery status, and pod restart detection (OOM).

## Maxmemory Testing

Measures `maxmemory-policy` behavior by writing more data than the cluster can retain
and checking eviction counters, write rejections, and a sample of accepted keys.

```bash
BACKUP_IMAGE=europe-central2-docker.pkg.dev/redis-vs-valkey/valkey-bench/backup_restore:1 \
VALKEY_MAXMEMORY=1gb \
MAXMEMORY_POLICIES="allkeys-lru volatile-lru" \
N=1 \
./k8s/scripts/maxmemory_benchmark.sh 4096 ./results/valkey_maxmemory
```

With 3 masters and `maxmemory 1gb`, `4096` MB of 1 KB values should push the cluster
past its retainable dataset size. `allkeys-lru` should continue by evicting keys.
`volatile-lru` will return OOM write errors for keys without TTL.

For Memorystore Redis Cluster:

```bash
BACKUP_IMAGE=europe-central2-docker.pkg.dev/redis-vs-valkey/valkey-bench/backup_restore:1 \
PROVIDER=memorystore \
MEMORYSTORE_CLUSTER_ID=redis-ms-2 \
MEMORYSTORE_MAXMEMORY=1073741824 \
MAXMEMORY_POLICIES="allkeys-lru volatile-lru" \
N=1 \
./k8s/scripts/maxmemory_benchmark.sh 4096 ./results/memorystore_maxmemory
```

Set `KEY_TTL_SECONDS=3600` to test `volatile-lru` with expiring keys.

To measure resilience while maxmemory pressure is happening, run memtier continuously
and start the maxmemory writer after the steady-state window:

```bash
N=1 ./k8s/scripts/maxmemory_resilience_benchmark.sh 4096 ./results/maxmemory_resilience
python cli.py resilience --input ./results/maxmemory_resilience --scenario maxmemory --output-dir ./plots/maxmemory_resilience
```

## Zero-Downtime Upgrade Testing

Measures whether a rolling update of the Valkey StatefulSet causes request errors or latency spikes.
A `helm upgrade` with a dummy annotation bump forces Kubernetes to restart all 6 pods one-by-one
(the same mechanism as a real version upgrade). Memtier runs continuous load throughout.

### Run upgrade benchmark

```bash
./k8s/scripts/upgrade_benchmark.sh ./results/upgrade
```

Override the Helm chart path, values file, or number of runs:

```bash
HELM_CHART_PATH=../valkey-helm/valkey VALUES_FILE=./k8s/manifests/values.yaml N=3 ./k8s/scripts/upgrade_benchmark.sh ./results/upgrade
```

The script starts a 5-minute memtier load, waits 30s for steady state, triggers the rolling
upgrade, and waits for both memtier and the rollout to finish before the next iteration.

### Analyse upgrade results

```bash
python cli.py upgrade --input ./results/upgrade --output-dir ./plots/upgrade
```

Produces per-run time series plots (ops/sec and latency with all disruption windows highlighted
in orange), a comparison bar chart, and `upgrade_summary.csv` with mean +/- std for:

- **Total disrupted time (ms)** — sum of all disruption window durations
- **Number of disruption events** — separate dips during the rolling restart
- **Total ops lost** — across all disruption windows
- **Max single disruption (ms)** — longest individual dip
- **Peak p99 during upgrade** vs baseline
- **Upgrade clean** — whether all individual disruptions were ≤2s (true zero-downtime)

## Data Consistency Testing

Tests whether acknowledged writes survive a network partition (split-brain scenario).
Uses a custom Python-based consistency checker instead of memtier -- it writes keys with
deterministic values, tracks ACKs, injects a Chaos Mesh `NetworkChaos` partition, then
verifies that every acknowledged key is still present after the partition heals.

### Build the consistency checker image

```bash
docker build -t consistency_checker:1 ./k8s/images/consistency
minikube image load consistency_checker:1
```

### Run consistency benchmark

```bash
./k8s/scripts/consistency_benchmark.sh ./results/consistency
```

Override the number of runs or image:

```bash
N=3 CONSISTENCY_IMAGE=consistency_checker:2 ./k8s/scripts/consistency_benchmark.sh ./results/consistency
```

Each run writes keys continuously for 120s. At ~30s, a 30s network partition isolates one
Valkey pod. After the write phase, the checker verifies all ACK'd keys and reports any missing
(ACK'd by the server but lost after partition recovery).

### Analyse consistency results

```bash
python cli.py consistency --input ./results/consistency --output-dir ./plots/consistency
```

Produces per-run write-rate time series (showing partition error windows), a comparison bar
chart, and `consistency_summary.csv` with mean +/- std for:

- **Keys missing** — acknowledged writes that were lost
- **Loss rate** — fraction of ACK'd writes that disappeared
- **Partition errors** — expected write failures during the partition
- **Write rate mean** — sustained writes/sec outside the partition window

## Horizontal Scaling / Resharding

Measures the operational impact of changing shard count in both directions under continuous
load. The test runs both slot-migration modes by default: legacy `valkey-cli --cluster
rebalance` with weights and atomic `valkey-cli --cluster rebalance` with
`--cluster-use-atomic-slot-migration`. For each mode it scales from 3 to 4
shards, moves slots onto the new shard, then runs a second workload while moving slots off
the extra shard and scaling back from 4 to 3 shards. Memtier captures the traffic impact
during slot migration (ASK/MOVED redirections, latency spikes).

### Run resharding benchmark

```bash
./k8s/scripts/reshard_benchmark.sh ./results/reshard
```

Each memtier phase runs for 120s by default. Override the Helm chart path, values file,
number of runs, migration modes, or test time:

```bash
HELM_CHART_PATH=../valkey-helm/valkey VALUES_FILE=./k8s/manifests/values.yaml N=3 TEST_TIME=120 ./k8s/scripts/reshard_benchmark.sh ./results/reshard
```

By default `RESHARD_MODES` is `legacy atomic`, so `N=5` produces five legacy runs and
five atomic runs. Use `RESHARD_MODES=legacy` or `RESHARD_MODES=atomic` to run only one mode.

For managed Memorystore for Redis Cluster, use the separate script. It starts the same
in-cluster memtier load pod, then triggers managed shard scaling through
`gcloud redis clusters`:

```bash
./k8s/scripts/memorystore_reshard_benchmark.sh redis-ms ./results/memorystore_reshard
```

The Memorystore script defaults to `europe-central2`, `3 -> 4 -> 3` shards, `N=5`, and
`TEST_TIME=900` because managed scaling can take several minutes. To target Memorystore
for Valkey instead, set `MEMORYSTORE_PRODUCT=valkey`.

Each run has two measured phases:

- **Reshard up**: starts memtier, waits 30s for steady state, checks that no Chaos Mesh
  experiment is active, patches the StatefulSet rolling-update partition to `6`, scales
  to 4 shards, waits for `valkey-6` and `valkey-7`, disables chart/init auto-rebalance
  for the measured upgrade, and explicitly moves slots onto the new master with weighted
  `--cluster rebalance`. Atomic mode adds `--cluster-use-atomic-slot-migration`.
- **Reshard down**: starts a second memtier run, waits 30s for steady state, moves slots
  off the extra master using the same migration mode, removes the extra cluster nodes, and
  only then scales the Helm release back to 3 shards.

The partition patch keeps existing pods `valkey-0` through `valkey-5` from being restarted
while the benchmark is measuring slot movement.

### Check cluster state

```bash
kubectl exec -n vk valkey-0 -- valkey-cli cluster info
kubectl exec -n vk valkey-0 -- valkey-cli cluster nodes
```

### Analyse resharding results

```bash
python cli.py reshard --input ./results/reshard --output-dir ./plots/reshard
```

To compare self-hosted legacy, self-hosted atomic, and managed Memorystore on the same
chart, put the result directories under one parent and analyse the parent:

```bash
python cli.py reshard --input ./results --output-dir ./plots/reshard
```

The comparison chart labels bars as `l-N` for legacy, `a-N` for atomic, and `ms-N` for
Memorystore. Memorystore operation time is shown as a single black-box segment because
node changes, slot movement, and stabilization are managed internally.

Produces per-phase time series plots (ops/sec and latency with reshard start/end markers
and line markers for scale/slot-migration subphases),
a duration-only stacked comparison chart (`reshard_comparison.png`) with reshard-up and
reshard-down panels side by side, and `reshard_summary.csv` with raw timing columns for:

- **Operation duration (s)** — total measured reshard operation time
- **Slot migration mode** — `legacy` for weighted `valkey-cli --cluster rebalance`,
  `atomic` for weighted rebalance plus `--cluster-use-atomic-slot-migration`
- **Scale / slot move duration (s)** — reshard-up timing breakdown
- **Move slots / delete nodes / scale-down duration (s)** — reshard-down timing breakdown
- **Wait/check duration (s)** — time between steps spent waiting for cluster health checks
- **Explicit rebalance status and slot count** — whether the explicit slot move completed
  and how many slots landed on the new master

Each up run also writes `reshard_<mode>_auto_rebalance_N.csv`, a one-row trace of slot
count on the new master before explicit migration starts.

## Backup & Restore Testing

Measures RDB save time and cluster recovery time after a full pod restart, parameterized by
dataset size. Seeds the cluster with a target amount of data, triggers `BGSAVE` on all masters,
kills all pods (PVCs are preserved), then measures the time until the cluster is healthy again
and verifies data integrity.

### Build the backup/restore image

```bash
docker build -t backup_restore:1 ./k8s/images/backup
minikube image load backup_restore:1
```

### Run backup/restore benchmark

Test with different dataset sizes (MB per shard):

```bash
./k8s/scripts/backup_restore_benchmark.sh 100 ./results/backup     # 100 MB/shard
./k8s/scripts/backup_restore_benchmark.sh 500 ./results/backup     # 500 MB/shard
./k8s/scripts/backup_restore_benchmark.sh 1000 ./results/backup    # 1 GB/shard
```

Override the number of runs or image:

```bash
N=5 BACKUP_IMAGE=backup_restore:2 ./k8s/scripts/backup_restore_benchmark.sh 100 ./results/backup
```

Each run seeds data, triggers BGSAVE, deletes all Valkey pods, waits for the cluster to
recover from RDB, verifies a 10% sample of keys, then cleans up. N=3 by default.

### Analyse backup/restore results

```bash
python cli.py backup --input ./results/backup --output-dir ./plots/backup
```

Produces a grouped bar chart (BGSAVE vs restore duration per size), a scatter plot of restore
time vs dataset size, and `backup_restore_summary.csv` with mean +/- std for:

- **Seed duration (s)** — time to populate the dataset
- **Save duration (s)** — BGSAVE wall time across all masters
- **Restore duration (s)** — time from pod deletion to cluster healthy
- **Data integrity** — whether all sampled keys were found after restore

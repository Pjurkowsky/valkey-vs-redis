# Valkey vs. Redis: Performance and Reliability Comparison of In-Memory Cache Clusters in Cloud-Native Architectures

---

### Currently working on:
https://github.com/valkey-io/valkey-helm/pull/116

---
## Stack Overview

- Kubernetes 
- Valkey Helm Chart
- Prometheus + Grafana (kube-prometheus-stack)
- ServiceMonitor enabled for metrics scraping
- memtier_benchmark (used as pod - created by custom docker image)

Valkey deployed in cluster mode:
- 3 shards
- 1 replica per shard
- Persistence enabled (5Gi)
- maxmemory (1Gi)

## Deployment

### Install Valkey


#### Clone helm chart repo
```bash
git clone https://github.com/valkey-io/valkey-helm.git
cd valkey-helm

git fetch origin pull/116/head:pr-116
git checkout pr-116
```

#### Install prometheus and grafana
```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

kubectl create namespace monitoring

helm install monitoring prometheus-community/kube-prometheus-stack -n monitoring
```

#### Port forward of grafana
```bash 
kubectl -n monitoring port-forward svc/monitoring-kube-prometheus-prometheus 9090:9090

```

#### Password for grafana
```bash
kubectl get secret -n monitoring monitoring-grafana -o jsonpath="{.data.admin-password}" | base64 -d; echo
```

#### Grafana dashboard
1. Click **+ → Import**  
2. Enter dashboard ID:
763
3. Click Load
4. Select Prometheus datasource
5. Import

#### Install chart
```bash
helm install valkey ../valkey-helm/valkey -n vk -f ./src/values.yaml --create-namespace
```

#### Build Docker image for benchmark pod
```bash
 docker build -t memtier_k8s:1 ./src
 minikube image load memtier_k8s:1      
 ```

 ```bash kubectl run memtier-shell -n vk --rm -it --restart=Never \
  --image=memtier_k8s:1 -- bash
    ```

#### Run python script
```bash
python main.py --input ./test/results_memtier
```